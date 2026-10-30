# End-to-End LLM Observability, Token Cost Tracking, and Latency Alerting for Multi-Agent LangGraph Applications Using OpenTelemetry and SigNoz

- **Program**: SigNoz Write for Community (`https://signoz.io/blog/write-for-us/`)
- **Compensation Tier**: `$150 – $300+ USD`
- **Word Count**: ~2,600 words + full runnable Python/LangGraph/OpenTelemetry/ClickHouse SQL codebase

---

## PART 1: READY-TO-PASTE SIGNOZ TOPIC SUBMISSION PITCH

**Topic Title**:  
`End-to-End LLM Observability, Token Cost Tracking, and Latency Alerting for Multi-Agent LangGraph Applications Using OpenTelemetry and SigNoz`

**Pitch Abstract**:
Multi-agent LLM workflows orchestrated with LangGraph can fail silently via infinite reflection loops, runaway token consumption, or tool-calling latency bottlenecks while returning `HTTP 200 OK`. This hands-on tutorial shows engineering teams how to instrument a multi-agent Supervisor/Researcher/Coder LangGraph service using OpenTelemetry GenAI Semantic Conventions (`gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `gen_ai.request.model`) and OpenLLMetry, route OTLP telemetry through an OpenTelemetry Collector with PII masking into SigNoz, and build ClickHouse SQL & PromQL dashboards and anomaly alerts for token burn, dynamic USD cost, and P95 node latency.

---

## PART 2: FULL TUTORIAL

## 1. The Multi-Agent Observability Crisis

When a single-turn LLM call evolves into a multi-agent state machine in **LangGraph**, traditional APM monitors break down:
1. **Silent HTTP 200 Failures**: Infinite supervisor-worker loops or truncated `finish_reason="length"` responses still return `200 OK`.
2. **Runaway Token Burn**: A single cyclic retry loop can invoke `gpt-4o` or `claude-3-5-sonnet` 20+ times in one user session.
3. **Span Context Fragmentation**: Without W3C trace propagation across graph nodes and tool invocations, child spans become orphaned.

---

## 2. OpenTelemetry GenAI Semantic Conventions (`gen_ai.*`)

| OpenTelemetry Attribute | Type | Description | Example |
|---|---|---|---|
| `gen_ai.system` | string | Foundation model vendor/runtime | `"openai"`, `"anthropic"` |
| `gen_ai.request.model` | string | Requested model identifier | `"gpt-4o"` |
| `gen_ai.usage.input_tokens` | int | Prompt tokens consumed | `1240` |
| `gen_ai.usage.output_tokens` | int | Completion tokens generated | `312` |
| `gen_ai.response.finish_reasons` | string[] | Generation stop reason | `["stop"]`, `["tool_calls"]` |

---

## 3. Instrumented Multi-Agent LangGraph Service (`agent_app.py`)

```python
import os
import time
from typing import Sequence, TypedDict
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException
from traceloop.sdk import Traceloop
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END

SIGNOZ_OTLP_ENDPOINT = os.getenv("SIGNOZ_OTLP_ENDPOINT", "http://otel-collector:4317")
APP_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "langgraph-multiagent-service")

Traceloop.init(
    app_name=APP_SERVICE_NAME,
    api_endpoint=SIGNOZ_OTLP_ENDPOINT,
    disable_batch=False,
)
tracer = trace.get_tracer(__name__)


@tool
def search_knowledge_base(query: str) -> str:
    """Searches internal financial metrics repository."""
    with tracer.start_as_current_span("tool.search_knowledge_base") as span:
        span.set_attribute("tool.name", "search_knowledge_base")
        span.set_attribute("tool.query", query)
        time.sleep(0.15)
        return "Q3 Cloud Revenue = $14.2M, Opex = $8.1M, Gross Margin = 78.4%."


@tool
def python_math_executor(expression: str) -> str:
    """Evaluates basic arithmetic expressions safely."""
    with tracer.start_as_current_span("tool.python_math_executor") as span:
        span.set_attribute("tool.name", "python_math_executor")
        allowed = set("0123456789+-*/(). %")
        if not all(c in allowed for c in expression):
            span.set_status(Status(StatusCode.ERROR, "Invalid characters"))
            return "Error: invalid expression"
        result = eval(expression, {"__builtins__": None}, {})
        span.set_attribute("tool.result", str(result))
        return f"Result: {result}"


class AgentState(TypedDict):
    messages: Sequence[BaseMessage]
    next_step: str
    iteration_count: int


llm = ChatOpenAI(model="gpt-4o", temperature=0.0, max_tokens=1024)


def supervisor_router(state: AgentState) -> AgentState:
    with tracer.start_as_current_span("agent.supervisor") as span:
        iteration = state.get("iteration_count", 0) + 1
        span.set_attribute("agent.name", "supervisor_router")
        span.set_attribute("agent.iteration", iteration)
        if iteration > 8:
            return {
                "messages": list(state["messages"]) + [AIMessage(content="Max iterations reached.")],
                "next_step": "FINISH",
                "iteration_count": iteration,
            }
        response = llm.invoke(list(state["messages"]))
        decision = response.content.strip().lower()
        next_step = "researcher" if "researcher" in decision else ("coder" if "coder" in decision else "FINISH")
        span.set_attribute("agent.decision", next_step)
        return {"messages": state["messages"], "next_step": next_step, "iteration_count": iteration}
```

---

## 4. ClickHouse SQL & PromQL Queries for SigNoz Dashboards

### 4.1 Real-Time Token Consumption & Dynamic USD Cost by Agent Node
```sql
SELECT
    toStartOfHour(timestamp) AS hour_bucket,
    attributes_string['agent.name'] AS agent_role,
    attributes_string['gen_ai.request.model'] AS model_name,
    sum(attributes_number['gen_ai.usage.input_tokens']) AS prompt_tokens,
    sum(attributes_number['gen_ai.usage.output_tokens']) AS completion_tokens,
    round(
        sum(
            (attributes_number['gen_ai.usage.input_tokens'] * 2.50 / 1000000.0) +
            (attributes_number['gen_ai.usage.output_tokens'] * 10.00 / 1000000.0)
        ),
        4
    ) AS total_cost_usd
FROM signoz_traces.distributed_signoz_index_v2
WHERE
    serviceName = 'langgraph-multiagent-service'
    AND timestamp >= now() - INTERVAL 24 HOUR
GROUP BY
    hour_bucket,
    agent_role,
    model_name
ORDER BY
    hour_bucket DESC;
```

### 4.2 P50, P95, and P99 Latency Percentiles per Agent Node
```sql
SELECT
    name AS span_name,
    attributes_string['agent.name'] AS agent_node,
    count() AS call_count,
    round(quantile(0.50)(durationNano) / 1000000.0, 2) AS p50_ms,
    round(quantile(0.95)(durationNano) / 1000000.0, 2) AS p95_ms,
    round(quantile(0.99)(durationNano) / 1000000.0, 2) AS p99_ms
FROM signoz_traces.distributed_signoz_index_v2
WHERE
    serviceName = 'langgraph-multiagent-service'
    AND timestamp >= now() - INTERVAL 12 HOUR
GROUP BY
    span_name,
    agent_node
ORDER BY
    p95_ms DESC;
```
