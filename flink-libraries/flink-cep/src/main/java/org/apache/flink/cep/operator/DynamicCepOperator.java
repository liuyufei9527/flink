/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.ComputationState;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Stream;

/**
 * CEP pattern operator for a keyed input stream. For each key, the operator creates
 * a {@link NFA} and a priority queue to buffer out of order elements. Both data structures are
 * stored using the managed keyed state.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT> Type of the output elements
 */
@Internal
public class DynamicCepOperator<IN, KEY, OUT>
		extends AbstractUdfStreamOperator<OUT, PatternProcessFunction<IN, OUT>>
	implements TwoInputStreamOperator<IN, Tuple2<String, Pattern<IN, ?>>, OUT>, Triggerable<KEY, VoidNamespace> {

	private static final long serialVersionUID = -4166778210774160757L;

	private final boolean isProcessingTime;

	private final TypeSerializer<IN> inputSerializer;

	///////////////			State			//////////////

	private static final String NFA_STATE_NAME = "nfaStateName";
	private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";

	//private final NFACompiler.NFAFactory<IN> nfaFactory;

	private transient ValueState<Map<String, NFAState>> computationStates;
	private transient MapState<Long, List<IN>> elementQueueState;

	private transient BroadcastState<String, Pattern<IN, ?>> patternBroadcastState;

	private transient SharedBuffer<IN> partialMatches;

	private transient InternalTimerService<VoidNamespace> timerService;

	private transient Map<String, NFA<IN>> nfas;

	/**
	 * The last seen watermark. This will be used to
	 * decide if an incoming element is late or not.
	 */
	private long lastWatermark;

	/** Comparator for secondary sorting. Primary sorting is always done on time. */
	private final EventComparator<IN> comparator;

	/**
	 * {@link OutputTag} to use for late arriving events. Elements with timestamp smaller than
	 * the current watermark will be emitted to this.
	 */
	private final OutputTag<IN> lateDataOutputTag;

	/** Strategy which element to skip after a match was found. */
	private transient Map<String, AfterMatchSkipStrategy> afterMatchSkipStrategy = new HashMap<>();

	/** Context passed to user function. */
	private transient ContextFunctionImpl context;

	/** Main output collector, that sets a proper timestamp to the StreamRecord. */
	private transient TimestampedCollector<OUT> collector;

	/** Wrapped RuntimeContext that limits the underlying context features. */
	private transient CepRuntimeContext cepRuntimeContext;

	/** Thin context passed to NFA that gives access to time related characteristics. */
	private transient TimerService cepTimerService;

	private final boolean timeoutHandling;

	public DynamicCepOperator(
			final TypeSerializer<IN> inputSerializer,
			final boolean isProcessingTime,
			@Nullable final EventComparator<IN> comparator,
			//@Nullable final AfterMatchSkipStrategy afterMatchSkipStrategy,
			final PatternProcessFunction<IN, OUT> function,
			@Nullable final OutputTag<IN> lateDataOutputTag,
			final boolean timeoutHandling) {
		super(function);

		this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
		//this.nfaFactory = Preconditions.checkNotNull(nfaFactory);
		this.isProcessingTime = isProcessingTime;
		this.comparator = comparator;
		this.lateDataOutputTag = lateDataOutputTag;
		this.timeoutHandling = timeoutHandling;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		this.cepRuntimeContext = new CepRuntimeContext(getRuntimeContext());
		FunctionUtils.setFunctionRuntimeContext(getUserFunction(), this.cepRuntimeContext);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		// initializeState through the provided context
		MapSerializer<String, NFAState> serializer = new MapSerializer<>(StringSerializer.INSTANCE, new NFAStateSerializer());
		computationStates = context.getKeyedStateStore().getState(
			new ValueStateDescriptor<>(
				NFA_STATE_NAME,
				serializer));

		partialMatches = new SharedBuffer<>(context.getKeyedStateStore(), inputSerializer);

		elementQueueState = context.getKeyedStateStore().getMapState(
				new MapStateDescriptor<>(
						EVENT_QUEUE_STATE_NAME,
						LongSerializer.INSTANCE,
						new ListSerializer<>(inputSerializer)));


		MapStateDescriptor<String, Pattern<IN, ?>> patternBroadcastStateDescriptors = new MapStateDescriptor<>(
			"pattern-broadcast-state",
			BasicTypeInfo.STRING_TYPE_INFO,
			TypeInformation.of(new TypeHint<Pattern<IN, ?>>() {}));
		this.patternBroadcastState = getOperatorStateBackend().getBroadcastState(patternBroadcastStateDescriptors);

		nfas = new HashMap<>();
		afterMatchSkipStrategy = new HashMap<>();

		Iterator<Map.Entry<String, Pattern<IN, ?>>> patternIterator = patternBroadcastState.iterator();
		while (patternIterator.hasNext()) {
			Map.Entry<String, Pattern<IN, ?>> patternEntry = patternIterator.next();
			String patternKey = patternEntry.getKey();
			Pattern<IN, ?> pattern = patternEntry.getValue();
			NFACompiler.NFAFactory<IN> nfaFactory = NFACompiler.compileFactory(pattern, timeoutHandling);
			NFA<IN> nfa = nfaFactory.createNFA();
			nfas.put(patternKey, nfa);
			AfterMatchSkipStrategy skipStrategy = pattern.getAfterMatchSkipStrategy();
			if (skipStrategy == null) {
				skipStrategy = AfterMatchSkipStrategy.noSkip();
			}
			afterMatchSkipStrategy.put(patternKey, skipStrategy);
		}
		//migrateOldState();
	}


	@Override
	public void open() throws Exception {
		super.open();
		timerService = getInternalTimerService(
				"watermark-callbacks",
				VoidNamespaceSerializer.INSTANCE,
				this);


		//nfa = nfaFactory.createNFA();
		//nfa.open(cepRuntimeContext, new Configuration());

		context = new ContextFunctionImpl();
		collector = new TimestampedCollector<>(output);
		cepTimerService = new TimerServiceImpl();
	}

	@Override
	public void close() throws Exception {
		super.close();
		for (Map.Entry<String, NFA<IN>> nfaEntry : nfas.entrySet()) {
			nfaEntry.getValue().close();
		}
	}


	@Override
	public void processElement1(StreamRecord<IN> element) throws Exception {
		LOG.info("processElement1");
		if (isProcessingTime) {
			/*if (comparator == null) {
				// there can be no out of order elements in processing time
				long timestamp = getProcessingTimeService().getCurrentProcessingTime();
				Map<String, NFAState> nfaStates = getNFAState();
				if (nfaStates == null || nfaStates.isEmpty()) {
					return;
					//todo
				}
				advanceTime(nfaStates, timestamp);
				processEvent(nfaStates, element.getValue(), timestamp);
				updateNFA(nfaStates);
			} else {*/
				long currentTime = timerService.currentProcessingTime();
				bufferEvent(element.getValue(), currentTime);

				// register a timer for the next millisecond to sort and emit buffered data
				timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, currentTime + 1);
			//}

		} else {

			long timestamp = element.getTimestamp();
			IN value = element.getValue();

			// In event-time processing we assume correctness of the watermark.
			// Events with timestamp smaller than or equal with the last seen watermark are considered late.
			// Late events are put in a dedicated side output, if the user has specified one.

			if (timestamp > lastWatermark) {

				// we have an event with a valid timestamp, so
				// we buffer it until we receive the proper watermark.

				saveRegisterWatermarkTimer();

				bufferEvent(value, timestamp);

			} else if (lateDataOutputTag != null) {
				output.collect(lateDataOutputTag, element);
			}
		}
	}

	@Override
	public void processElement2(StreamRecord<Tuple2<String, Pattern<IN, ?>>> element) throws Exception {
		LOG.info("processElement2");
		String patternKey = element.getValue().f0;
		Pattern<IN, ?> pattern = element.getValue().f1;

		if (pattern == null) {
			patternBroadcastState.remove(patternKey);
			nfas.remove(patternKey);
			afterMatchSkipStrategy.remove(patternKey);
		} else {
			Pattern<IN, ?> oldPattern = patternBroadcastState.get(patternKey);
			long version = oldPattern == null ? 1L : oldPattern.getVersion() + 1;
			pattern.setVersion(version);

			NFA<IN> oldNfa = nfas.get(patternKey);
			NFACompiler.NFAFactory<IN> nfaFactory = NFACompiler.compileFactory(pattern, timeoutHandling);
			NFA<IN> nfa = nfaFactory.createNFA();
			if (!nfa.equals(oldNfa)) {
				nfas.put(patternKey, nfa);
				AfterMatchSkipStrategy skipStrategy = pattern.getAfterMatchSkipStrategy();
				if (skipStrategy == null) {
					skipStrategy = AfterMatchSkipStrategy.noSkip();
				}
				afterMatchSkipStrategy.put(patternKey, skipStrategy);
				patternBroadcastState.put(patternKey, pattern);
			}
		}

		/*if (pattern == null) {
			nfas.remove(patternKey);
			//release previous event when a pattern unloaded
			releasePatternStates(patternKey);
		} else {
			NFACompiler.NFAFactory<IN> nfaFactory = NFACompiler.compileFactory(pattern, false);
			NFA<IN> nfa = nfaFactory.createNFA();
			NFA<IN> oldNfa = nfas.get(patternKey);
			if (!nfa.equals(oldNfa)) {
				//discard state
				nfas.put(patternKey, nfa);
				releasePatternStates(patternKey);

				NFAState initialNFAState = nfa.createInitialNFAState();
				initialNFAState.setStateChanged();
				Map<String, NFAState> stateMap = getNFAState();
				stateMap.put(patternKey, initialNFAState);
				updateNFA(stateMap);
			}

		}*/
	}


	private void releasePatternStates(String patternKey) throws Exception {
		NFAState nfaState = computationStates.value().get(patternKey);
		if (nfaState != null) {
			for (ComputationState computationState : nfaState.getPartialMatches()) {
				NodeId nodeId = computationState.getPreviousBufferEntry();
				try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
					sharedBufferAccessor.releaseNode(nodeId);
				}
			}
		}
	}

	/**
	 * Registers a timer for {@code current watermark + 1}, this means that we get triggered
	 * whenever the watermark advances, which is what we want for working off the queue of
	 * buffered elements.
	 */
	private void saveRegisterWatermarkTimer() {
		long currentWatermark = timerService.currentWatermark();
		// protect against overflow
		if (currentWatermark + 1 > currentWatermark) {
			timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, currentWatermark + 1);
		}
	}

	private void bufferEvent(IN event, long currentTime) throws Exception {
		List<IN> elementsForTimestamp =  elementQueueState.get(currentTime);
		if (elementsForTimestamp == null) {
			elementsForTimestamp = new ArrayList<>();
		}

		if (getExecutionConfig().isObjectReuseEnabled()) {
			// copy the StreamRecord so that it cannot be changed
			elementsForTimestamp.add(inputSerializer.copy(event));
		} else {
			elementsForTimestamp.add(event);
		}
		elementQueueState.put(currentTime, elementsForTimestamp);
	}

	@Override
	public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {

		// 1) get the queue of pending elements for the key and the corresponding NFA,
		// 2) process the pending elements in event time order and custom comparator if exists
		//		by feeding them in the NFA
		// 3) advance the time to the current watermark, so that expired patterns are discarded.
		// 4) update the stored state for the key, by only storing the new NFA and MapState iff they
		//		have state to be used later.
		// 5) update the last seen watermark.

		// STEP 1
		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();

		Map<String, NFAState> nfaStates = getNFAState();
		if (nfaStates == null) {
			saveRegisterWatermarkTimer();
			return;
		}

		// STEP 2

		while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= timerService.currentWatermark()) {
			long timestamp = sortedTimestamps.poll();
			advanceTime(nfaStates, timestamp);
			try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
				elements.forEachOrdered(
					event -> {
						try {
							processEvent(nfaStates, event, timestamp);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				);
			}
			elementQueueState.remove(timestamp);
		}

		// STEP 3
		advanceTime(nfaStates, timerService.currentWatermark());

		if (!sortedTimestamps.isEmpty() || !partialMatches.isEmpty()) {
			saveRegisterWatermarkTimer();
		}

		// STEP4
		updateNFA(nfaStates);

		// STEP 5
		updateLastSeenWatermark(timerService.currentWatermark());
	}

	@Override
	public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
		// 1) get the queue of pending elements for the key and the corresponding NFA,
		// 2) process the pending elements in process time order and custom comparator if exists
		//		by feeding them in the NFA
		// 3) update the stored state for the key, by only storing the new NFA and MapState iff they
		//		have state to be used later.

		// STEP 1
		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
		Map<String, NFAState> nfaStates = getNFAState();

		if (nfaStates == null) {
			long currentTime = timerService.currentProcessingTime();
			timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, currentTime + 1);
			return;
		}

		// STEP 2
		while (!sortedTimestamps.isEmpty()) {
			long timestamp = sortedTimestamps.poll();
			advanceTime(nfaStates, timestamp);
			try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
				elements.forEachOrdered(
					event -> {
						try {
							processEvent(nfaStates, event, timestamp);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				);
			}
			elementQueueState.remove(timestamp);
		}

		// STEP 3
		updateNFA(nfaStates);
	}

	private Stream<IN> sort(Collection<IN> elements) {
		Stream<IN> stream = elements.stream();
		return (comparator == null) ? stream : stream.sorted(comparator);
	}

	private void updateLastSeenWatermark(long timestamp) {
		this.lastWatermark = timestamp;
	}

	private Map<String, NFAState> getNFAState() throws Exception {
		Map<String, NFAState> nfaStates = computationStates.value();
		if (nfaStates == null) {
			nfaStates = new HashMap<>();
		}
		if (nfas == null || nfas.isEmpty()) {
			return null;
		}
		for (String patternKey : nfas.keySet()) {
			if (nfaStates.get(patternKey) == null) {
				nfaStates.put(patternKey, nfas.get(patternKey).createInitialNFAState());
			} else {
				NFAState nfaState = nfaStates.get(patternKey);
				NFA<IN> nfa = nfas.get(patternKey);
				if (nfa == null) {
					releasePatternStates(patternKey);
					nfaStates.remove(patternKey);
				} else if (nfaState.getVersion() != nfa.getVersion()) {
					releasePatternStates(patternKey);
					nfaState = nfa.createInitialNFAState();
					nfaStates.put(patternKey, nfaState);
				}
			}

		}
		return nfaStates;
	}

	private void updateNFA(Map<String, NFAState> nfaStates) throws IOException {
		if (nfaStates == null) {
			return;
		}
		boolean changed = false;
		for (Map.Entry<String, NFAState> stateEntry : nfaStates.entrySet()) {
			NFAState nfaState = stateEntry.getValue();
			if (nfaState.isStateChanged()) {
				nfaState.resetStateChanged();
				changed = true;
			}
		}
		if (changed) {
			computationStates.update(nfaStates);
		}
	}

	private PriorityQueue<Long> getSortedTimestamps() throws Exception {
		PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
		for (Long timestamp : elementQueueState.keys()) {
			sortedTimestamps.offer(timestamp);
		}
		return sortedTimestamps;
	}

	/**
	 * Process the given event by giving it to the NFA and outputting the produced set of matched
	 * event sequences.
	 *
	 * @param event The current event to be processed
	 * @param timestamp The timestamp of the event
	 */
	private void processEvent(Map<String, NFAState> nfaStates, IN event, long timestamp) throws Exception {
		//Map<String, NFAState> nfaStates = computationStates.value();
		/*if (nfaStates == null) {
			//buffer events until have any pattern
			bufferEvent(event, timestamp);
			if (isProcessingTime) {
				timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, timestamp + 100);
			} else {
				saveRegisterWatermarkTimer();
			}
			return;
		}*/

		try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
			for (String patternKey : nfaStates.keySet()) {
				NFA<IN> nfa = nfas.get(patternKey);

				// when nfa updated, but process old nfaState
				NFAState nfaState = nfaStates.get(patternKey);

				Collection<Map<String, List<IN>>> patterns =
					nfa.process(sharedBufferAccessor, nfaState, event, timestamp, afterMatchSkipStrategy.get(patternKey), cepTimerService);
				processMatchedSequences(patterns, timestamp);
			}
		}
	}

	/**
	 * Advances the time for the given NFA to the given timestamp. This means that no more events with timestamp
	 * <b>lower</b> than the given timestamp should be passed to the nfa, This can lead to pruning and timeouts.
	 */
	private void advanceTime(Map<String, NFAState> nfaStates, long timestamp) throws Exception {
		if (nfaStates == null) {
			return;
		}
		for (Map.Entry<String, NFAState> stateEntry : nfaStates.entrySet()) {
			NFAState nfaState = stateEntry.getValue();
			NFA<IN> nfa = nfas.get(stateEntry.getKey());
			try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
				Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut =
					nfa.advanceTime(sharedBufferAccessor, nfaState, timestamp);
				if (!timedOut.isEmpty()) {
					processTimedOutSequences(timedOut);
				}
			}
		}
		updateNFA(nfaStates);
	}

	private void processMatchedSequences(Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {
		PatternProcessFunction<IN, OUT> function = getUserFunction();
		setTimestamp(timestamp);
		for (Map<String, List<IN>> matchingSequence : matchingSequences) {
			function.processMatch(matchingSequence, context, collector);
		}
	}

	private void processTimedOutSequences(Collection<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences) throws Exception {
		PatternProcessFunction<IN, OUT> function = getUserFunction();
		if (function instanceof TimedOutPartialMatchHandler) {

			@SuppressWarnings("unchecked")
			TimedOutPartialMatchHandler<IN> timeoutHandler = (TimedOutPartialMatchHandler<IN>) function;

			for (Tuple2<Map<String, List<IN>>, Long> matchingSequence : timedOutSequences) {
				setTimestamp(matchingSequence.f1);
				timeoutHandler.processTimedOutMatch(matchingSequence.f0, context);
			}
		}
	}

	private void setTimestamp(long timestamp) {
		if (!isProcessingTime) {
			collector.setAbsoluteTimestamp(timestamp);
		}

		context.setTimestamp(timestamp);
	}

	/**
	 * Gives {@link NFA} access to {@link InternalTimerService} and tells if {@link DynamicCepOperator} works in
	 * processing time. Should be instantiated once per operator.
	 */
	private class TimerServiceImpl implements TimerService {

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}

	}

	/**
	 * Implementation of {@link PatternProcessFunction.Context}. Design to be instantiated once per operator.
	 * It serves three methods:
	 *  <ul>
	 *      <li>gives access to currentProcessingTime through {@link InternalTimerService}</li>
	 *      <li>gives access to timestamp of current record (or null if Processing time)</li>
	 *      <li>enables side outputs with proper timestamp of StreamRecord handling based on either Processing or
	 *          Event time</li>
	 *  </ul>
	 */
	private class ContextFunctionImpl implements PatternProcessFunction.Context {

		private Long timestamp;

		@Override
		public <X> void output(final OutputTag<X> outputTag, final X value) {
			final StreamRecord<X> record;
			if (isProcessingTime) {
				record = new StreamRecord<>(value);
			} else {
				record = new StreamRecord<>(value, timestamp());
			}
			output.collect(outputTag, record);
		}

		void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		@Override
		public long timestamp() {
			return timestamp;
		}

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}
	}

}
