use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::vec::Vec;

use crate::plan::is_nursery_gc;
use crate::scheduler::ProcessEdgesWork;
use crate::scheduler::WorkBucketStage;
use crate::util::ObjectReference;
use crate::util::VMWorkerThread;
use crate::vm::ReferenceGlue;
use crate::vm::VMBinding;

/// Holds all reference processors for each weak reference Semantics.
/// Currently this is based on Java's weak reference semantics (soft/weak/phantom).
/// We should make changes to make this general rather than Java specific.
pub struct ReferenceProcessors {
    soft: ReferenceProcessor,
    weak: ReferenceProcessor,
    phantom: ReferenceProcessor,
}

impl ReferenceProcessors {
    pub fn new() -> Self {
        ReferenceProcessors {
            soft: ReferenceProcessor::new(Semantics::SOFT),
            weak: ReferenceProcessor::new(Semantics::WEAK),
            phantom: ReferenceProcessor::new(Semantics::PHANTOM),
        }
    }

    pub fn get(&self, semantics: Semantics) -> &ReferenceProcessor {
        match semantics {
            Semantics::SOFT => &self.soft,
            Semantics::WEAK => &self.weak,
            Semantics::PHANTOM => &self.phantom,
        }
    }

    pub fn add_soft_candidate(&self, reff: ObjectReference) {
        trace!("Add soft candidate: {}", reff);
        self.soft.add_candidate(reff);
    }

    pub fn add_weak_candidate(&self, reff: ObjectReference) {
        trace!("Add weak candidate: {}", reff);
        self.weak.add_candidate(reff);
    }

    pub fn add_phantom_candidate(&self, reff: ObjectReference) {
        trace!("Add phantom candidate: {}", reff);
        self.phantom.add_candidate(reff);
    }

    /// This will invoke enqueue for each reference processor, which will
    /// call back to the VM to enqueue references whose referents are cleared
    /// in this GC.
    pub fn enqueue_refs<VM: VMBinding>(&self, tls: VMWorkerThread) {
        self.soft.enqueue::<VM>(tls);
        self.weak.enqueue::<VM>(tls);
        self.phantom.enqueue::<VM>(tls);
    }

    /// A separate reference forwarding step. Normally when we scan refs, we deal with forwarding.
    /// However, for some plans like mark compact, at the point we do ref scanning, we do not know
    /// the forwarding addresses yet, thus we cannot do forwarding during scan refs. And for those
    /// plans, this separate step is required.
    pub fn forward_refs<E: ProcessEdgesWork>(&self, trace: &mut E, mmtk: &'static MMTK<E::VM>) {
        debug_assert!(
            mmtk.get_plan().constraints().needs_forward_after_liveness,
            "A plan with needs_forward_after_liveness=false does not need a separate forward step"
        );
        self.soft
            .forward::<E>(trace, is_nursery_gc(mmtk.get_plan()));
        self.weak
            .forward::<E>(trace, is_nursery_gc(mmtk.get_plan()));
        self.phantom
            .forward::<E>(trace, is_nursery_gc(mmtk.get_plan()));
    }

    // Methods for scanning weak references. It needs to be called in a decreasing order of reference strengths, i.e. soft > weak > phantom

    pub fn retain_soft_refs<E: ProcessEdgesWork>(&self, trace: &mut E, mmtk: &'static MMTK<E::VM>) {
        self.soft.retain::<E>(trace, is_nursery_gc(mmtk.get_plan()));
    }

    /// Scan soft references.
    pub fn scan_soft_refs<VM: VMBinding>(&self, mmtk: &'static MMTK<VM>) {
        // This will update the references (and the referents).
        self.soft.scan::<VM>(is_nursery_gc(mmtk.get_plan()));
    }

    /// Scan weak references.
    pub fn scan_weak_refs<VM: VMBinding>(&self, mmtk: &'static MMTK<VM>) {
        self.weak.scan::<VM>(is_nursery_gc(mmtk.get_plan()));
    }

    /// Scan phantom references.
    pub fn scan_phantom_refs<VM: VMBinding>(&self, mmtk: &'static MMTK<VM>) {
        self.phantom.scan::<VM>(is_nursery_gc(mmtk.get_plan()));
    }
}

impl Default for ReferenceProcessors {
    fn default() -> Self {
        Self::new()
    }
}

// XXX: We differ from the original implementation
//      by ignoring "stress," i.e. where the array
//      of references is grown by 1 each time. We
//      can't do this here b/c std::vec::Vec doesn't
//      allow us to customize its behaviour like that.
//      (Similarly, GROWTH_FACTOR is locked at 2.0, but
//      luckily this is also the value used by Java MMTk.)
const INITIAL_SIZE: usize = 256;

/// We create a reference processor for each semantics. Generally we expect these
/// to happen for each processor:
/// 1. The VM adds reference candidates. They could either do it when a weak reference
///    is created, or when a weak reference is traced during GC.
/// 2. We scan references after the GC determins liveness.
/// 3. We forward references if the GC needs forwarding after liveness.
/// 4. We inform the binding of references whose referents are cleared during this GC by enqueue'ing.
pub struct ReferenceProcessor {
    /// Most of the reference processor is protected by a mutex.
    sync: Mutex<ReferenceProcessorSync>,

    /// The semantics for the reference processor
    semantics: Semantics,

    /// Is it allowed to add candidate to this reference processor? The value is true for most of the time,
    /// but it is set to false once we finish forwarding references, at which point we do not expect to encounter
    /// any 'new' reference in the same GC. This makes sure that no new entry will be added to our reference table once
    /// we finish forwarding, as we will not be able to process the entry in that GC.
    // This avoids an issue in the following scenario in mark compact:
    // 1. First trace: add a candidate WR
    // 2. Weak reference scan: scan the reference table, as MC does not forward object in the first trace. This scan does not update any reference.
    // 3. Second trace: call add_candidate again with WR, but WR gets ignored as we already have WR in our reference table.
    // 4. Weak reference forward: call trace_object for WR, which pushes WR to the node buffer and update WR -> WR' in our reference table.
    // 5. When we trace objects in the node buffer, we will attempt to add WR as a candidate. As we have updated WR to WR' in our reference
    //    table, we would accept WR as a candidate. But we will not trace WR again, and WR will be invalid after this GC.
    // This flag is set to false after Step 4, so in Step 5, we will ignore adding WR.
    allow_new_candidate: AtomicBool,
}

#[derive(Debug, PartialEq)]
pub enum Semantics {
    SOFT,
    WEAK,
    PHANTOM,
}

struct ReferenceProcessorSync {
    /// The table of reference objects for the current semantics. We add references to this table by
    /// add_candidate(). After scanning this table, a reference in the table should either
    /// stay in the table (if the referent is alive) or go to enqueued_reference (if the referent is dead and cleared).
    /// Note that this table should not have duplicate entries, otherwise we will scan the duplicates multiple times, and
    /// that may lead to incorrect results.
    references: HashSet<ObjectReference>,

    /// References whose referents are cleared during this GC. We add references to this table during
    /// scanning, and we pop from this table during the enqueue work at the end of GC.
    enqueued_references: Vec<ObjectReference>,

    /// Index into the references table for the start of nursery objects
    nursery_index: usize,
}

impl ReferenceProcessor {
    pub fn new(semantics: Semantics) -> Self {
        ReferenceProcessor {
            sync: Mutex::new(ReferenceProcessorSync {
                references: HashSet::with_capacity(INITIAL_SIZE),
                enqueued_references: vec![],
                nursery_index: 0,
            }),
            semantics,
            allow_new_candidate: AtomicBool::new(true),
        }
    }

    /// Add a candidate.
    pub fn add_candidate(&self, reff: ObjectReference) {
        if !self.allow_new_candidate.load(Ordering::SeqCst) {
            return;
        }

        let mut sync = self.sync.lock().unwrap();
        sync.references.insert(reff);
    }

    fn disallow_new_candidate(&self) {
        self.allow_new_candidate.store(false, Ordering::SeqCst);
    }

    fn allow_new_candidate(&self) {
        self.allow_new_candidate.store(true, Ordering::SeqCst);
    }

    // These functions call `ObjectReference::get_forwarded_object`, not `trace_object()`.
    // They are used by steps that do not expand the transitive closure.  Processing weak and
    // phantom references never expand the transitive closure.  Soft references, when not retained,
    // do not expand the transitive closure, either.
    // These functions are intended to make the code easier to understand.

    /// Return the new `ObjectReference` of a referent if it is already moved, or its current
    /// `ObjectReference` otherwise.  The referent must be live when calling this function.
    fn get_forwarded_referent(referent: ObjectReference) -> ObjectReference {
        debug_assert!(referent.is_live());
        referent.get_forwarded_object().unwrap_or(referent)
    }

    /// Return the new `ObjectReference` of a reference object if it is already moved, or its
    /// current `ObjectReference` otherwise.  The reference object must be live when calling this
    /// function.
    fn get_forwarded_reference(object: ObjectReference) -> ObjectReference {
        debug_assert!(object.is_live());
        object.get_forwarded_object().unwrap_or(object)
    }

    // These funcions call `trace_object()`, which will ensure the object and its descendents will
    // be traced.  They are only called in steps that expand the transitive closure.  That include
    // retaining soft references, and (for MarkCompact) tracing objects for forwarding.
    // Note that finalizers also expand the transitive closure.
    // These functions are intended to make the code easier to understand.

    /// This function is called when retaining soft reference.  It
    /// -   keeps the referent alive, and
    /// -   adds the referent to the tracing queue if not yet reached, so that its children will be
    ///     kept alive, too, and
    /// -   gets the new object reference of the referent if it is moved.
    fn keep_referent_alive<E: ProcessEdgesWork>(
        e: &mut E,
        referent: ObjectReference,
    ) -> ObjectReference {
        e.trace_object(referent)
    }

    /// This function is called when forwarding the references and referents (for MarkCompact). It
    /// -   adds the reference or the referent to the tracing queue if not yet reached, so that
    ///     the children of the reference or referent will be visited and forwarded, too, and
    /// -   gets the forwarded object reference of the object.
    fn trace_forward_object<E: ProcessEdgesWork>(
        e: &mut E,
        referent: ObjectReference,
    ) -> ObjectReference {
        e.trace_object(referent)
    }

    /// Inform the binding to enqueue the weak references whose referents were cleared in this GC.
    pub fn enqueue<VM: VMBinding>(&self, tls: VMWorkerThread) {
        let mut sync = self.sync.lock().unwrap();

        // This is the end of a GC. We do some assertions here to make sure our reference tables are correct.
        #[cfg(debug_assertions)]
        {
            // For references in the table, the reference needs to be valid, and if the referent is not cleared, it should be valid as well
            sync.references.iter().for_each(|reff| {
                debug_assert!(reff.is_in_any_space());
                if let Some(referent) = VM::VMReferenceGlue::get_referent(*reff) {
                    debug_assert!(
                        referent.is_in_any_space(),
                        "Referent {:?} (of reference {:?}) is not in any space",
                        referent,
                        reff
                    );
                }
            });
            // For references that will be enqueue'd, the reference needs to be valid, and the referent needs to be cleared.
            sync.enqueued_references.iter().for_each(|reff| {
                debug_assert!(reff.is_in_any_space());
                let maybe_referent = VM::VMReferenceGlue::get_referent(*reff);
                debug_assert!(maybe_referent.is_none());
            });
        }

        if !sync.enqueued_references.is_empty() {
            trace!("enqueue: {:?}", sync.enqueued_references);
            VM::VMReferenceGlue::enqueue_references(&sync.enqueued_references, tls);
            sync.enqueued_references.clear();
        }

        self.allow_new_candidate();
    }

    /// Forward the reference tables in the reference processor. This is only needed if a plan does not forward
    /// objects in their first transitive closure.
    /// nursery is not used for this.
    pub fn forward<E: ProcessEdgesWork>(&self, trace: &mut E, _nursery: bool) {
        let mut sync = self.sync.lock().unwrap();
        debug!("Starting ReferenceProcessor.forward({:?})", self.semantics);

        // Forward a single reference
        fn forward_reference<E: ProcessEdgesWork>(
            trace: &mut E,
            reference: ObjectReference,
        ) -> ObjectReference {
            {
                use crate::vm::ObjectModel;
                trace!(
                    "Forwarding reference: {} (size: {})",
                    reference,
                    <E::VM as VMBinding>::VMObjectModel::get_current_size(reference)
                );
            }

            if let Some(old_referent) =
                <E::VM as VMBinding>::VMReferenceGlue::get_referent(reference)
            {
                let new_referent = ReferenceProcessor::trace_forward_object(trace, old_referent);
                <E::VM as VMBinding>::VMReferenceGlue::set_referent(reference, new_referent);

                trace!(
                    " referent: {} (forwarded to {})",
                    old_referent,
                    new_referent
                );
            }

            let new_reference = ReferenceProcessor::trace_forward_object(trace, reference);
            trace!(" reference: forwarded to {}", new_reference);

            new_reference
        }

        sync.references = sync
            .references
            .iter()
            .map(|reff| forward_reference::<E>(trace, *reff))
            .collect();

        sync.enqueued_references = sync
            .enqueued_references
            .iter()
            .map(|reff| forward_reference::<E>(trace, *reff))
            .collect();

        debug!("Ending ReferenceProcessor.forward({:?})", self.semantics);

        // We finish forwarding. No longer accept new candidates.
        self.disallow_new_candidate();
    }

    /// Scan the reference table, and update each reference/referent.
    /// It doesn't keep the reference or the referent alive.
    // TODO: nursery is currently ignored. We used to use Vec for the reference table, and use an int
    // to point to the reference that we last scanned. However, when we use HashSet for reference table,
    // we can no longer do that.
    fn scan<VM: VMBinding>(&self, _nursery: bool) {
        let mut sync = self.sync.lock().unwrap();

        debug!("Starting ReferenceProcessor.scan({:?})", self.semantics);

        trace!(
            "{:?} Reference table is {:?}",
            self.semantics,
            sync.references
        );

        //debug_assert!(sync.enqueued_references.is_empty());
        // Put enqueued reference in this vec
        let mut enqueued_references = vec![];

        // Determinine liveness for each reference and only keep the refs if `process_reference()` returns Some.
        let new_set: HashSet<ObjectReference> = sync
            .references
            .iter()
            .filter_map(|reff| self.process_reference::<VM>(*reff, &mut enqueued_references))
            .collect();

        debug!(
            "{:?} reference table from {} to {} ({} enqueued)",
            self.semantics,
            sync.references.len(),
            new_set.len(),
            enqueued_references.len()
        );
        sync.references = new_set;
        sync.enqueued_references.extend(enqueued_references);

        debug!("Ending ReferenceProcessor.scan({:?})", self.semantics);
    }

    /// Retain referent in the reference table. This method deals only with soft references.
    /// It retains the referent if the reference is definitely reachable. This method does
    /// not update reference or referent. So after this method, scan() should be used to update
    /// the references/referents.
    fn retain<E: ProcessEdgesWork>(&self, trace: &mut E, _nursery: bool) {
        debug_assert!(self.semantics == Semantics::SOFT);

        let sync = self.sync.lock().unwrap();

        debug!("Starting ReferenceProcessor.retain({:?})", self.semantics);
        trace!(
            "{:?} Reference table is {:?}",
            self.semantics,
            sync.references
        );

        for reference in sync.references.iter() {
            trace!("Processing reference: {:?}", reference);

            if !reference.is_live() {
                // Reference is currently unreachable but may get reachable by the
                // following trace. We postpone the decision.
                continue;
            }
            // Reference is definitely reachable.  Retain the referent.
            if let Some(referent) = <E::VM as VMBinding>::VMReferenceGlue::get_referent(*reference)
            {
                Self::keep_referent_alive(trace, referent);
                trace!(" ~> {:?} (retained)", referent);
            }
        }

        debug!("Ending ReferenceProcessor.retain({:?})", self.semantics);
    }

    /// Process a reference.
    /// * If both the reference and the referent is alive, return the updated reference and update its referent properly.
    /// * If the reference is alive, and the referent is not cleared but not alive, return None and the reference (with cleared referent) is enqueued.
    /// * For other cases, return None.
    ///
    /// If a None value is returned, the reference can be removed from the reference table. Otherwise, the updated reference should be kept
    /// in the reference table.
    fn process_reference<VM: VMBinding>(
        &self,
        reference: ObjectReference,
        enqueued_references: &mut Vec<ObjectReference>,
    ) -> Option<ObjectReference> {
        trace!("Process reference: {}", reference);

        // If the reference is dead, we're done with it. Let it (and
        // possibly its referent) be garbage-collected.
        if !reference.is_live() {
            VM::VMReferenceGlue::clear_referent(reference);
            trace!(" UNREACHABLE reference: {}", reference);
            return None;
        }

        // The reference object is live.
        let new_reference = Self::get_forwarded_reference(reference);
        trace!(" forwarded to: {}", new_reference);

        // Get the old referent.
        let maybe_old_referent = VM::VMReferenceGlue::get_referent(reference);
        trace!(" referent: {:?}", maybe_old_referent);

        // If the application has cleared the referent the Java spec says
        // this does not cause the Reference object to be enqueued. We
        // simply allow the Reference object to fall out of our
        // waiting list.
        let Some(old_referent) = maybe_old_referent else {
            trace!("  (cleared referent) ");
            return None;
        };

        if old_referent.is_live() {
            // Referent is still reachable in a way that is as strong as
            // or stronger than the current reference level.
            let new_referent = Self::get_forwarded_referent(old_referent);
            debug_assert!(new_referent.is_live());
            trace!("  forwarded referent to: {}", new_referent);

            // The reference object stays on the waiting list, and the
            // referent is untouched. The only thing we must do is
            // ensure that the former addresses are updated with the
            // new forwarding addresses in case the collector is a
            // copying collector.

            // Update the referent
            VM::VMReferenceGlue::set_referent(new_reference, new_referent);
            Some(new_reference)
        } else {
            // Referent is unreachable. Clear the referent and enqueue the reference object.
            trace!("  UNREACHABLE referent: {}", old_referent);

            VM::VMReferenceGlue::clear_referent(new_reference);
            enqueued_references.push(new_reference);
            None
        }
    }
}

use crate::scheduler::GCWork;
use crate::scheduler::GCWorker;
use crate::MMTK;
use std::marker::PhantomData;

#[derive(Default)]
pub(crate) struct RescanReferences<VM: VMBinding> {
    pub soft: bool,
    pub weak: bool,
    pub phantom_data: PhantomData<VM>,
}

impl<VM: VMBinding> GCWork<VM> for RescanReferences<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        if self.soft {
            mmtk.reference_processors.scan_soft_refs(mmtk);
        }
        if self.weak {
            mmtk.reference_processors.scan_weak_refs(mmtk);
        }
    }
}

#[derive(Default)]
pub(crate) struct SoftRefProcessing<E: ProcessEdgesWork>(PhantomData<E>);
impl<E: ProcessEdgesWork> GCWork<E::VM> for SoftRefProcessing<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        if !mmtk.state.is_emergency_collection() {
            // Postpone the scanning to the end of the transitive closure from strongly reachable
            // soft references.
            let rescan = Box::new(RescanReferences {
                soft: true,
                weak: false,
                phantom_data: PhantomData,
            });
            worker.scheduler().work_buckets[WorkBucketStage::SoftRefClosure].set_sentinel(rescan);

            // Retain soft references.  This will expand the transitive closure.  We create an
            // instance of `E` for this.
            let mut w = E::new(vec![], false, mmtk, WorkBucketStage::SoftRefClosure);
            w.set_worker(worker);
            mmtk.reference_processors.retain_soft_refs(&mut w, mmtk);
            w.flush();
        } else {
            // Scan soft references immediately without retaining.
            mmtk.reference_processors.scan_soft_refs(mmtk);
        }
    }
}
impl<E: ProcessEdgesWork> SoftRefProcessing<E> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

#[derive(Default)]
pub(crate) struct WeakRefProcessing<VM: VMBinding>(PhantomData<VM>);
impl<VM: VMBinding> GCWork<VM> for WeakRefProcessing<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        mmtk.reference_processors.scan_weak_refs(mmtk);
    }
}
impl<VM: VMBinding> WeakRefProcessing<VM> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

#[derive(Default)]
pub(crate) struct PhantomRefProcessing<VM: VMBinding>(PhantomData<VM>);
impl<VM: VMBinding> GCWork<VM> for PhantomRefProcessing<VM> {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        mmtk.reference_processors.scan_phantom_refs(mmtk);
    }
}
impl<VM: VMBinding> PhantomRefProcessing<VM> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

#[derive(Default)]
pub(crate) struct RefForwarding<E: ProcessEdgesWork>(PhantomData<E>);
impl<E: ProcessEdgesWork> GCWork<E::VM> for RefForwarding<E> {
    fn do_work(&mut self, worker: &mut GCWorker<E::VM>, mmtk: &'static MMTK<E::VM>) {
        let mut w = E::new(vec![], false, mmtk, WorkBucketStage::RefForwarding);
        w.set_worker(worker);
        mmtk.reference_processors.forward_refs(&mut w, mmtk);
        w.flush();
    }
}
impl<E: ProcessEdgesWork> RefForwarding<E> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

#[derive(Default)]
pub(crate) struct RefEnqueue<VM: VMBinding>(PhantomData<VM>);
impl<VM: VMBinding> GCWork<VM> for RefEnqueue<VM> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        mmtk.reference_processors.enqueue_refs::<VM>(worker.tls);
    }
}
impl<VM: VMBinding> RefEnqueue<VM> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}
