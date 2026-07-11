from agents.haian_dwh_agent.models import AgentGoal, AgentRunState, ToolResult


def test_tool_result_success_contract():
    result = ToolResult.success(
        tool="check_data_quality",
        summary="No errors",
        data={"table": "fact_booking", "errors": 0},
    )

    assert result.status == "ok"
    assert result.tool == "check_data_quality"
    assert result.data["table"] == "fact_booking"
    assert result.errors == []


def test_tool_result_failure_contract():
    result = ToolResult.failure(
        tool="sync_table_to_bigquery",
        summary="FK violation",
        errors=["missing dim_room_type id=999"],
        data={"table": "fact_booking"},
    )

    assert result.status == "error"
    assert result.errors == ["missing dim_room_type id=999"]
    assert result.data["table"] == "fact_booking"


def test_agent_run_state_limits_actions():
    state = AgentRunState(goal=AgentGoal.DAILY_COMPETITOR_PIPELINE, max_actions=2)
    state = state.record_action("check_data_quality")
    state = state.record_action("sync_table_to_bigquery")

    assert state.action_count == 2
    assert state.has_action_budget is False


def test_agent_goal_behaves_like_string_enum():
    assert AgentGoal.DAILY_COMPETITOR_PIPELINE == "daily_competitor_pipeline"
    assert AgentGoal.DAILY_COMPETITOR_PIPELINE.value == "daily_competitor_pipeline"
    assert str(AgentGoal.DAILY_COMPETITOR_PIPELINE) == "daily_competitor_pipeline"
