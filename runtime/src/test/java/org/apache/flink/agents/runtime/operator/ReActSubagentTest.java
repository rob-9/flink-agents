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

package org.apache.flink.agents.runtime.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.agents.api.Event;
import org.apache.flink.agents.api.InputEvent;
import org.apache.flink.agents.api.OutputEvent;
import org.apache.flink.agents.api.agents.Agent;
import org.apache.flink.agents.api.agents.ReActAgent;
import org.apache.flink.agents.api.chat.messages.ChatMessage;
import org.apache.flink.agents.api.chat.messages.MessageRole;
import org.apache.flink.agents.api.chat.model.BaseChatModelConnection;
import org.apache.flink.agents.api.chat.model.BaseChatModelSetup;
import org.apache.flink.agents.api.context.RunnerContext;
import org.apache.flink.agents.api.resource.ResourceContext;
import org.apache.flink.agents.api.resource.ResourceDescriptor;
import org.apache.flink.agents.api.resource.ResourceType;
import org.apache.flink.agents.api.subagent.SubagentResult;
import org.apache.flink.agents.api.subagent.SubagentSetup;
import org.apache.flink.agents.api.tools.Tool;
import org.apache.flink.agents.plan.AgentPlan;
import org.apache.flink.agents.plan.actions.Action;
import org.apache.flink.agents.runtime.actionstate.ActionState;
import org.apache.flink.agents.runtime.actionstate.ActionStateSerde;
import org.apache.flink.agents.runtime.actionstate.InMemoryActionStateStore;
import org.apache.flink.agents.runtime.subagent.InternalSubagentCallEvent;
import org.apache.flink.agents.runtime.subagent.InternalSubagentSetup;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Exercises the actual scoped ReAct chat/tool loop without an external model. */
@Timeout(30)
public class ReActSubagentTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> CALLS = new ArrayList<>();
    private static boolean structured;

    public static class Answer {
        public String answer;
    }

    @BeforeEach
    void reset() {
        assumeTrue(Runtime.version().feature() >= 21, "Internal Java calls need continuations");
        CALLS.clear();
        structured = false;
    }

    public static class ScriptedSetup extends BaseChatModelSetup {
        public ScriptedSetup(ResourceDescriptor descriptor, ResourceContext context) {
            super(descriptor, context);
        }

        @Override
        public Map<String, Object> getParameters() {
            return new HashMap<>(Map.of("model", getModel()));
        }
    }

    public static class ScriptedConnection extends BaseChatModelConnection {
        public ScriptedConnection(ResourceDescriptor descriptor, ResourceContext context) {
            super(descriptor, context);
        }

        @Override
        public ChatMessage chat(
                List<ChatMessage> messages, List<Tool> tools, Map<String, Object> params) {
            String model = (String) params.get("model");
            ChatMessage last = messages.get(messages.size() - 1);
            CALLS.add(model + ":" + last.getRole().getValue());
            if (last.getRole() == MessageRole.TOOL) {
                assertThat(last.getContent())
                        .contains(model.equals("child") ? "child evidence" : "child answer");
                return new ChatMessage(
                        MessageRole.ASSISTANT,
                        structured && model.equals("child")
                                ? "{\"answer\":\"child answer\"}"
                                : model + " answer");
            }
            if (model.equals("child")) {
                assertThat(messages.get(0).getContent()).isEqualTo("Literal {prompt} instructions");
                assertThat(last.getContent()).isEqualTo("investigate");
                assertThat(tools).extracting(Tool::getName).containsExactly("evidence");
                return request("evidence", Map.of());
            }
            assertThat(tools).extracting(Tool::getName).containsExactly("_subagent_researcher");
            assertThat(tools.get(0).getDescription()).contains("Research a task");
            return request("_subagent_researcher", Map.of("prompt", "investigate"));
        }
    }

    public static class ChildTools {
        @org.apache.flink.agents.api.annotation.Tool(description = "Child evidence")
        public static String evidence() {
            return "child evidence";
        }
    }

    public static class ParentTools {
        @org.apache.flink.agents.api.annotation.Tool(description = "Parent evidence")
        public static String evidence() {
            return "parent evidence";
        }
    }

    private static ChatMessage request(String name, Map<String, Object> arguments) {
        return ChatMessage.assistant(
                "",
                List.of(
                        Map.of(
                                "id",
                                "call-1",
                                "type",
                                "function",
                                "function",
                                Map.of("name", name, "arguments", arguments))));
    }

    private static ResourceDescriptor model(String name) {
        ResourceDescriptor.Builder builder =
                ResourceDescriptor.Builder.newBuilder(ScriptedSetup.class.getName())
                        .addInitialArgument("connection", "connection")
                        .addInitialArgument("model", name);
        if (name.equals("child")) {
            builder.addInitialArgument("tools", List.of("evidence"));
        } else {
            builder.addInitialArgument("subagents", List.of("researcher"));
        }
        return builder.build();
    }

    private static Agent child() throws Exception {
        Agent child =
                ReActAgent.forSubagent(
                        model("child"),
                        "Research a task",
                        "Literal {prompt} instructions",
                        structured ? Answer.class : null);
        child.addAction(
                new String[] {InputEvent.EVENT_TYPE},
                ReActSubagentTest.class.getMethod(
                        "assertChildScope", Event.class, RunnerContext.class));
        child.addResource(
                "evidence",
                ResourceType.TOOL,
                Tool.fromMethod(ChildTools.class.getMethod("evidence")));
        return child;
    }

    private static void resources(Agent parent) throws Exception {
        parent.addResource(
                "connection",
                ResourceType.CHAT_MODEL_CONNECTION,
                new ResourceDescriptor(ScriptedConnection.class.getName(), Map.of()));
        parent.addResource(
                "evidence",
                ResourceType.TOOL,
                Tool.fromMethod(ParentTools.class.getMethod("evidence")));
        parent.addResource("researcher", ResourceType.AGENT, child());
    }

    public static void assertChildScope(Event event, RunnerContext ctx) throws Exception {
        assertThat(ctx.getSensoryMemory().isExist("parent_secret")).isFalse();
        assertThat(ctx.getSensoryMemory().isExist("child_marker")).isFalse();
        ctx.getSensoryMemory().set("child_marker", "private");
    }

    public static void invokeTwice(Event event, RunnerContext ctx) throws Exception {
        invoke(event, ctx);
        invoke(event, ctx);
    }

    public static void invoke(Event event, RunnerContext ctx) throws Exception {
        SubagentSetup child = (SubagentSetup) ctx.getResource("researcher", ResourceType.AGENT);
        ctx.getSensoryMemory().set("parent_secret", "private");
        Object input = InputEvent.fromEvent(event).getInput();
        SubagentResult result =
                child.submit(
                                ctx,
                                input.equals(2L)
                                        ? Map.of("prompt", 42)
                                        : Map.of("prompt", "investigate"))
                        .await();
        assertThat(ctx.getSensoryMemory().isExist("child_marker")).isFalse();
        assertThat(ctx.getSensoryMemory().isExist("_TOOL_CALL_CONTEXT")).isFalse();
        ctx.sendEvent(
                new OutputEvent(
                        result.isSuccess() ? result.getResult() : result.getErrorMessage()));
    }

    private static Agent explicitParent() throws Exception {
        Agent parent = new Agent();
        parent.addAction(
                new String[] {InputEvent.EVENT_TYPE},
                ReActSubagentTest.class.getMethod("invoke", Event.class, RunnerContext.class));
        resources(parent);
        return parent;
    }

    @Test
    void explicitCallRunsTheChildToolLoopAfterPlanReconstruction() throws Exception {
        assertThat(run(new AgentPlan(explicitParent()), 1L))
                .containsExactly(List.of("child answer"));
        assertThat(CALLS).containsExactly("child:user", "child:tool");
    }

    @Test
    void modelDelegationResumesTheParentWithTheChildAnswer() throws Exception {
        Agent parent = new ReActAgent(model("parent"), null, null);
        resources(parent);
        assertThat(run(new AgentPlan(parent), 1L)).containsExactly("parent answer");
        assertThat(CALLS).containsExactly("parent:user", "child:user", "child:tool", "parent:tool");
    }

    @Test
    void invalidPromptIsAChildFailureAndDoesNotCallTheModel() throws Exception {
        List<Object> output = run(new AgentPlan(explicitParent()), 2L);
        assertThat(output).hasSize(1);
        assertThat(output.get(0).toString()).contains("only a string 'prompt'");
        assertThat(CALLS).isEmpty();
    }

    @Test
    void successiveCallsHaveIndependentChildMemory() throws Exception {
        Agent parent = new Agent();
        parent.addAction(
                new String[] {InputEvent.EVENT_TYPE},
                ReActSubagentTest.class.getMethod("invokeTwice", Event.class, RunnerContext.class));
        resources(parent);
        assertThat(run(new AgentPlan(parent), 1L))
                .containsExactly(List.of("child answer"), List.of("child answer"));
        assertThat(CALLS).containsExactly("child:user", "child:tool", "child:user", "child:tool");
    }

    @Test
    void structuredChildOutputIsNormalizedForModelDelegation() throws Exception {
        structured = true;
        Agent parent = new ReActAgent(model("parent"), null, null);
        resources(parent);
        assertThat(run(new AgentPlan(parent), 1L)).containsExactly("parent answer");
    }

    /** Persist values by copy, as a durable backend does, before later actions mutate memory. */
    private static class SnapshotStore extends InMemoryActionStateStore {
        SnapshotStore() {
            super(false);
        }

        @Override
        public void put(Object key, long seq, Action action, Event event, ActionState state)
                throws IOException {
            super.put(
                    key,
                    seq,
                    action,
                    event,
                    ActionStateSerde.deserialize(ActionStateSerde.serialize(state)));
        }
    }

    @Test
    void replayRestoresChildMemoryBeforeResumingItsToolLoop() throws Exception {
        InMemoryActionStateStore first = new SnapshotStore();
        assertThat(run(new AgentPlan(explicitParent()), 1L, first))
                .containsExactly(List.of("child answer"));
        assertThat(CALLS).containsExactly("child:user", "child:tool");
        Map<String, ActionState> recoveredStates = new LinkedHashMap<>();
        first.getKeyedActionStates()
                .get(1L)
                .forEach(
                        (key, state) -> {
                            if (state.getTaskEvent() instanceof InternalSubagentCallEvent) {
                                InternalSubagentCallEvent envelope =
                                        (InternalSubagentCallEvent) state.getTaskEvent();
                                // Retain the initial child request's completed actions, but drop
                                // the tool call
                                // and later actions. The restored tool response needs the earlier
                                // memory writes.
                                if (envelope.getDelegateEventType().equals(InputEvent.EVENT_TYPE)
                                        || envelope.getDelegateEventType()
                                                .equals("_chat_request_event")) {
                                    recoveredStates.put(
                                            key,
                                            ActionStateSerde.deserialize(
                                                    ActionStateSerde.serialize(state)));
                                }
                            }
                        });
        assertThat(recoveredStates).hasSize(3);
        InMemoryActionStateStore recovered = new InMemoryActionStateStore(false);
        recovered.getKeyedActionStates().put(1L, recoveredStates);
        assertThat(run(new AgentPlan(explicitParent()), 1L, recovered))
                .containsExactly(List.of("child answer"));
        assertThat(CALLS).containsExactly("child:user", "child:tool", "child:tool");
    }

    @Test
    void failedChildRemainsFailedWhenOnlyItsStateWasPersisted() throws Exception {
        InMemoryActionStateStore first = new SnapshotStore();
        List<Object> failure = run(new AgentPlan(explicitParent()), 2L, first);
        assertThat(failure.get(0).toString()).contains("only a string 'prompt'");
        Map<String, ActionState> childStates = new LinkedHashMap<>();
        first.getKeyedActionStates()
                .get(2L)
                .forEach(
                        (key, state) -> {
                            if (state.getTaskEvent() instanceof InternalSubagentCallEvent) {
                                childStates.put(
                                        key,
                                        ActionStateSerde.deserialize(
                                                ActionStateSerde.serialize(state)));
                            }
                        });
        assertThat(childStates.values())
                .anyMatch(state -> state.getSubagentFailureMessage() != null);
        InMemoryActionStateStore recovered = new InMemoryActionStateStore(false);
        recovered.getKeyedActionStates().put(2L, childStates);
        assertThat(run(new AgentPlan(explicitParent()), 2L, recovered)).isEqualTo(failure);
        assertThat(CALLS).isEmpty();
    }

    @Test
    void descriptorRoundTripPreservesCallableMetadataAndChildPlan() throws Exception {
        AgentPlan plan = new AgentPlan(explicitParent());
        InternalSubagentSetup setup =
                (InternalSubagentSetup)
                        plan.getResourceProviders()
                                .get(ResourceType.AGENT)
                                .get("researcher")
                                .provide(null);
        ResourceDescriptor descriptor =
                MAPPER.readValue(
                        MAPPER.writeValueAsString(setup.getDescriptor()), ResourceDescriptor.class);
        InternalSubagentSetup restored = new InternalSubagentSetup(descriptor, null);
        assertThat(restored.getDescription()).isEqualTo("Research a task");
        assertThat(MAPPER.readTree(restored.getInputSchema()).path("required").get(0).asText())
                .isEqualTo("prompt");
        assertThat(restored.getChildPlan().getActions()).containsKey("startAction");
    }

    @SuppressWarnings("unchecked")
    private static List<Object> run(AgentPlan plan, long input) throws Exception {
        return run(plan, input, new InMemoryActionStateStore(false));
    }

    @SuppressWarnings("unchecked")
    private static List<Object> run(AgentPlan plan, long input, InMemoryActionStateStore store)
            throws Exception {
        AgentPlan restored = MAPPER.readValue(MAPPER.writeValueAsString(plan), AgentPlan.class);
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Object> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new ActionExecutionOperatorFactory<>(restored, true, store),
                        (KeySelector<Long, Long>) value -> value,
                        TypeInformation.of(Long.class))) {
            harness.open();
            harness.processElement(new StreamRecord<>(input));
            ((ActionExecutionOperator<Long, Object>) harness.getOperator())
                    .waitInFlightEventsFinished();
            return ((List<StreamRecord<Object>>) harness.getRecordOutput())
                    .stream().map(StreamRecord::getValue).collect(Collectors.toList());
        }
    }
}
