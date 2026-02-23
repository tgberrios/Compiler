#include "catalog/workflow_repository.h"
#include "catalog/WorkflowVersionManager.h"
#include "core/logger.h"
#include <algorithm>
#include <stdexcept>
#include <sstream>
#include <iomanip>
#include <ctime>

WorkflowRepository::WorkflowRepository(std::string connectionString)
    : connectionString_(std::move(connectionString)) {}

pqxx::connection WorkflowRepository::getConnection() {
  return pqxx::connection(connectionString_);
}

void WorkflowRepository::createTables() {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    
    txn.exec(
        "CREATE SCHEMA IF NOT EXISTS metadata;"
        "CREATE TABLE IF NOT EXISTS metadata.workflows ("
        "    id SERIAL PRIMARY KEY,"
        "    workflow_name VARCHAR(255) UNIQUE NOT NULL,"
        "    description TEXT,"
        "    schedule_cron VARCHAR(100),"
        "    active BOOLEAN DEFAULT true,"
        "    enabled BOOLEAN DEFAULT true,"
        "    retry_policy JSONB DEFAULT '{\"max_retries\": 3, \"retry_delay_seconds\": 60, \"retry_backoff_multiplier\": 2}'::jsonb,"
        "    sla_config JSONB DEFAULT '{\"max_execution_time_seconds\": 3600, \"alert_on_sla_breach\": true}'::jsonb,"
        "    metadata JSONB DEFAULT '{}'::jsonb,"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    updated_at TIMESTAMP DEFAULT NOW(),"
        "    last_execution_time TIMESTAMP,"
        "    last_execution_status VARCHAR(50)"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.workflow_tasks ("
        "    id SERIAL PRIMARY KEY,"
        "    workflow_name VARCHAR(255) NOT NULL REFERENCES metadata.workflows(workflow_name) ON DELETE CASCADE,"
        "    task_name VARCHAR(255) NOT NULL,"
        "    task_type VARCHAR(50) NOT NULL CHECK (task_type IN ('CUSTOM_JOB', 'DATA_WAREHOUSE', 'DATA_VAULT', 'SYNC', 'API_CALL', 'SCRIPT')),"
        "    task_reference VARCHAR(255) NOT NULL,"
        "    description TEXT,"
        "    task_config JSONB DEFAULT '{}'::jsonb,"
        "    retry_policy JSONB DEFAULT '{\"max_retries\": 3, \"retry_delay_seconds\": 60}'::jsonb,"
        "    position_x INTEGER DEFAULT 0,"
        "    position_y INTEGER DEFAULT 0,"
        "    metadata JSONB DEFAULT '{}'::jsonb,"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    updated_at TIMESTAMP DEFAULT NOW(),"
        "    CONSTRAINT uq_workflow_task UNIQUE (workflow_name, task_name)"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.workflow_dependencies ("
        "    id SERIAL PRIMARY KEY,"
        "    workflow_name VARCHAR(255) NOT NULL REFERENCES metadata.workflows(workflow_name) ON DELETE CASCADE,"
        "    upstream_task_name VARCHAR(255) NOT NULL,"
        "    downstream_task_name VARCHAR(255) NOT NULL,"
        "    dependency_type VARCHAR(50) DEFAULT 'SUCCESS' CHECK (dependency_type IN ('SUCCESS', 'COMPLETION', 'SKIP_ON_FAILURE')),"
        "    condition_expression TEXT,"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    CONSTRAINT uq_workflow_dependency UNIQUE (workflow_name, upstream_task_name, downstream_task_name),"
        "    CONSTRAINT chk_different_tasks CHECK (upstream_task_name != downstream_task_name),"
        "    FOREIGN KEY (workflow_name, upstream_task_name) REFERENCES metadata.workflow_tasks(workflow_name, task_name) ON DELETE CASCADE,"
        "    FOREIGN KEY (workflow_name, downstream_task_name) REFERENCES metadata.workflow_tasks(workflow_name, task_name) ON DELETE CASCADE"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.workflow_executions ("
        "    id BIGSERIAL PRIMARY KEY,"
        "    workflow_name VARCHAR(255) NOT NULL REFERENCES metadata.workflows(workflow_name) ON DELETE CASCADE,"
        "    execution_id VARCHAR(255) UNIQUE NOT NULL,"
        "    status VARCHAR(50) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED', 'SKIPPED')),"
        "    trigger_type VARCHAR(50) DEFAULT 'SCHEDULED' CHECK (trigger_type IN ('SCHEDULED', 'MANUAL', 'API', 'EVENT')),"
        "    start_time TIMESTAMP,"
        "    end_time TIMESTAMP,"
        "    duration_seconds INTEGER,"
        "    total_tasks INTEGER DEFAULT 0,"
        "    completed_tasks INTEGER DEFAULT 0,"
        "    failed_tasks INTEGER DEFAULT 0,"
        "    skipped_tasks INTEGER DEFAULT 0,"
        "    error_message TEXT,"
        "    metadata JSONB DEFAULT '{}'::jsonb,"
        "    created_at TIMESTAMP DEFAULT NOW()"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.workflow_task_executions ("
        "    id BIGSERIAL PRIMARY KEY,"
        "    workflow_execution_id BIGINT NOT NULL REFERENCES metadata.workflow_executions(id) ON DELETE CASCADE,"
        "    workflow_name VARCHAR(255) NOT NULL,"
        "    task_name VARCHAR(255) NOT NULL,"
        "    status VARCHAR(50) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED', 'SKIPPED', 'RETRYING')),"
        "    start_time TIMESTAMP,"
        "    end_time TIMESTAMP,"
        "    duration_seconds INTEGER,"
        "    retry_count INTEGER DEFAULT 0,"
        "    error_message TEXT,"
        "    task_output JSONB DEFAULT '{}'::jsonb,"
        "    metadata JSONB DEFAULT '{}'::jsonb,"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    FOREIGN KEY (workflow_name, task_name) REFERENCES metadata.workflow_tasks(workflow_name, task_name) ON DELETE CASCADE"
        ");"
        "CREATE TABLE IF NOT EXISTS metadata.workflow_versions ("
        "    id SERIAL PRIMARY KEY,"
        "    workflow_name VARCHAR(255) NOT NULL REFERENCES metadata.workflows(workflow_name) ON DELETE CASCADE,"
        "    version INTEGER NOT NULL,"
        "    description TEXT,"
        "    created_by VARCHAR(255),"
        "    is_current BOOLEAN DEFAULT false,"
        "    workflow_definition JSONB NOT NULL,"
        "    created_at TIMESTAMP DEFAULT NOW(),"
        "    CONSTRAINT uq_workflow_version UNIQUE (workflow_name, version)"
        ");"
    );
    
    txn.exec(
        "CREATE INDEX IF NOT EXISTS idx_workflows_active ON metadata.workflows(active);"
        "CREATE INDEX IF NOT EXISTS idx_workflows_enabled ON metadata.workflows(enabled);"
        "CREATE INDEX IF NOT EXISTS idx_workflows_schedule ON metadata.workflows(schedule_cron) WHERE schedule_cron IS NOT NULL;"
        "CREATE INDEX IF NOT EXISTS idx_workflow_tasks_workflow ON metadata.workflow_tasks(workflow_name);"
        "CREATE INDEX IF NOT EXISTS idx_workflow_dependencies_workflow ON metadata.workflow_dependencies(workflow_name);"
        "CREATE INDEX IF NOT EXISTS idx_workflow_executions_workflow ON metadata.workflow_executions(workflow_name);"
        "CREATE INDEX IF NOT EXISTS idx_workflow_executions_status ON metadata.workflow_executions(status);"
        "CREATE INDEX IF NOT EXISTS idx_workflow_executions_start_time ON metadata.workflow_executions(start_time DESC);"
        "CREATE INDEX IF NOT EXISTS idx_workflow_task_executions_execution ON metadata.workflow_task_executions(workflow_execution_id);"
        "CREATE INDEX IF NOT EXISTS idx_workflow_versions_workflow ON metadata.workflow_versions(workflow_name);"
    );
    
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "createTables",
                  "Error creating workflow tables: " + std::string(e.what()));
    throw;
  }
}

std::string WorkflowRepository::taskTypeToString(TaskType type) {
  switch (type) {
  case TaskType::CUSTOM_JOB:
    return "CUSTOM_JOB";
  case TaskType::DATA_WAREHOUSE:
    return "DATA_WAREHOUSE";
  case TaskType::DATA_VAULT:
    return "DATA_VAULT";
  case TaskType::SYNC:
    return "SYNC";
  case TaskType::API_CALL:
    return "API_CALL";
  case TaskType::SCRIPT:
    return "SCRIPT";
  default:
    return "CUSTOM_JOB";
  }
}

TaskType WorkflowRepository::stringToTaskType(const std::string &str) {
  if (str == "CUSTOM_JOB")
    return TaskType::CUSTOM_JOB;
  if (str == "DATA_WAREHOUSE")
    return TaskType::DATA_WAREHOUSE;
  if (str == "DATA_VAULT")
    return TaskType::DATA_VAULT;
  if (str == "SYNC")
    return TaskType::SYNC;
  if (str == "API_CALL")
    return TaskType::API_CALL;
  if (str == "SCRIPT")
    return TaskType::SCRIPT;
  if (str == "SUB_WORKFLOW")
    return TaskType::SUB_WORKFLOW;
  return TaskType::CUSTOM_JOB;
}

std::string WorkflowRepository::dependencyTypeToString(DependencyType type) {
  switch (type) {
  case DependencyType::SUCCESS:
    return "SUCCESS";
  case DependencyType::COMPLETION:
    return "COMPLETION";
  case DependencyType::SKIP_ON_FAILURE:
    return "SKIP_ON_FAILURE";
  default:
    return "SUCCESS";
  }
}

DependencyType WorkflowRepository::stringToDependencyType(const std::string &str) {
  if (str == "SUCCESS")
    return DependencyType::SUCCESS;
  if (str == "COMPLETION")
    return DependencyType::COMPLETION;
  if (str == "SKIP_ON_FAILURE")
    return DependencyType::SKIP_ON_FAILURE;
  return DependencyType::SUCCESS;
}

std::string WorkflowRepository::executionStatusToString(ExecutionStatus status) {
  switch (status) {
  case ExecutionStatus::PENDING:
    return "PENDING";
  case ExecutionStatus::RUNNING:
    return "RUNNING";
  case ExecutionStatus::SUCCESS:
    return "SUCCESS";
  case ExecutionStatus::FAILED:
    return "FAILED";
  case ExecutionStatus::CANCELLED:
    return "CANCELLED";
  case ExecutionStatus::SKIPPED:
    return "SKIPPED";
  case ExecutionStatus::RETRYING:
    return "RETRYING";
  default:
    return "PENDING";
  }
}

ExecutionStatus WorkflowRepository::stringToExecutionStatus(const std::string &str) {
  if (str == "PENDING")
    return ExecutionStatus::PENDING;
  if (str == "RUNNING")
    return ExecutionStatus::RUNNING;
  if (str == "SUCCESS")
    return ExecutionStatus::SUCCESS;
  if (str == "FAILED")
    return ExecutionStatus::FAILED;
  if (str == "CANCELLED")
    return ExecutionStatus::CANCELLED;
  if (str == "SKIPPED")
    return ExecutionStatus::SKIPPED;
  if (str == "RETRYING")
    return ExecutionStatus::RETRYING;
  return ExecutionStatus::PENDING;
}

std::string WorkflowRepository::triggerTypeToString(TriggerType type) {
  switch (type) {
  case TriggerType::SCHEDULED:
    return "SCHEDULED";
  case TriggerType::MANUAL:
    return "MANUAL";
  case TriggerType::API:
    return "API";
  case TriggerType::EVENT:
    return "EVENT";
  default:
    return "SCHEDULED";
  }
}

TriggerType WorkflowRepository::stringToTriggerType(const std::string &str) {
  if (str == "SCHEDULED")
    return TriggerType::SCHEDULED;
  if (str == "MANUAL")
    return TriggerType::MANUAL;
  if (str == "API")
    return TriggerType::API;
  if (str == "EVENT")
    return TriggerType::EVENT;
  return TriggerType::SCHEDULED;
}

RetryPolicy WorkflowRepository::parseRetryPolicy(const json &j) {
  RetryPolicy policy;
  if (j.contains("max_retries"))
    policy.max_retries = j["max_retries"].get<int>();
  if (j.contains("retry_delay_seconds"))
    policy.retry_delay_seconds = j["retry_delay_seconds"].get<int>();
  if (j.contains("retry_backoff_multiplier"))
    policy.retry_backoff_multiplier = j["retry_backoff_multiplier"].get<double>();
  return policy;
}

SLAConfig WorkflowRepository::parseSLAConfig(const json &j) {
  SLAConfig config;
  if (j.contains("max_execution_time_seconds"))
    config.max_execution_time_seconds = j["max_execution_time_seconds"].get<int>();
  if (j.contains("alert_on_sla_breach"))
    config.alert_on_sla_breach = j["alert_on_sla_breach"].get<bool>();
  return config;
}

RollbackConfig WorkflowRepository::parseRollbackConfig(const json &j) {
  RollbackConfig config;
  if (j.contains("enabled"))
    config.enabled = j["enabled"].get<bool>();
  if (j.contains("on_failure"))
    config.on_failure = j["on_failure"].get<bool>();
  if (j.contains("on_timeout"))
    config.on_timeout = j["on_timeout"].get<bool>();
  if (j.contains("max_rollback_depth"))
    config.max_rollback_depth = j["max_rollback_depth"].get<int>();
  return config;
}

json WorkflowRepository::retryPolicyToJson(const RetryPolicy &policy) {
  json j;
  j["max_retries"] = policy.max_retries;
  j["retry_delay_seconds"] = policy.retry_delay_seconds;
  j["retry_backoff_multiplier"] = policy.retry_backoff_multiplier;
  return j;
}

json WorkflowRepository::slaConfigToJson(const SLAConfig &config) {
  json j;
  j["max_execution_time_seconds"] = config.max_execution_time_seconds;
  j["alert_on_sla_breach"] = config.alert_on_sla_breach;
  return j;
}

json WorkflowRepository::rollbackConfigToJson(const RollbackConfig &config) {
  json j;
  j["enabled"] = config.enabled;
  j["on_failure"] = config.on_failure;
  j["on_timeout"] = config.on_timeout;
  j["max_rollback_depth"] = config.max_rollback_depth;
  return j;
}

std::string WorkflowRepository::rollbackStatusToString(RollbackStatus status) {
  switch (status) {
    case RollbackStatus::PENDING:
      return "PENDING";
    case RollbackStatus::IN_PROGRESS:
      return "IN_PROGRESS";
    case RollbackStatus::COMPLETED:
      return "COMPLETED";
    case RollbackStatus::FAILED:
      return "FAILED";
    default:
      return "PENDING";
  }
}

std::vector<WorkflowModel> WorkflowRepository::getAllWorkflows() {
  std::vector<WorkflowModel> workflows;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT id, workflow_name, description, schedule_cron, active, enabled, "
        "retry_policy, sla_config, COALESCE(rollback_config, '{\"enabled\": false}'::jsonb) as rollback_config, "
        "metadata, created_at, updated_at, "
        "last_execution_time, last_execution_status "
        "FROM metadata.workflows ORDER BY workflow_name");

    for (const auto &row : results) {
      WorkflowModel workflow = rowToWorkflow(row);
      
      pqxx::params taskParams;
      taskParams.append(workflow.workflow_name);
      auto taskResults = txn.exec(
          pqxx::zview("SELECT id, workflow_name, task_name, task_type, task_reference, "
          "description, task_config, retry_policy, position_x, position_y, "
          "metadata, COALESCE(priority, 0) as priority, "
          "COALESCE(condition_type, 'ALWAYS') as condition_type, "
          "condition_expression, parent_condition_task_name, "
          "loop_type, loop_config, created_at, updated_at "
          "FROM metadata.workflow_tasks WHERE workflow_name = $1"),
          taskParams);
      
      for (const auto &taskRow : taskResults) {
        workflow.tasks.push_back(rowToTask(taskRow));
      }
      
      pqxx::params depParams;
      depParams.append(workflow.workflow_name);
      auto depResults = txn.exec(
          pqxx::zview("SELECT id, workflow_name, upstream_task_name, downstream_task_name, "
          "dependency_type, condition_expression, created_at "
          "FROM metadata.workflow_dependencies WHERE workflow_name = $1"),
          depParams);
      
      for (const auto &depRow : depResults) {
        workflow.dependencies.push_back(rowToDependency(depRow));
      }
      
      workflows.push_back(workflow);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getAllWorkflows",
                  "Error getting workflows: " + std::string(e.what()));
  }
  return workflows;
}

std::vector<WorkflowModel> WorkflowRepository::getActiveWorkflows() {
  std::vector<WorkflowModel> workflows;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    auto results = txn.exec(
        "SELECT id, workflow_name, description, schedule_cron, active, enabled, "
        "retry_policy, sla_config, metadata, created_at, updated_at, "
        "last_execution_time, last_execution_status "
        "FROM metadata.workflows WHERE active = true AND enabled = true "
        "ORDER BY workflow_name");

    for (const auto &row : results) {
      WorkflowModel workflow = rowToWorkflow(row);
      
      pqxx::params taskParams;
      taskParams.append(workflow.workflow_name);
      auto taskResults = txn.exec(
          pqxx::zview("SELECT id, workflow_name, task_name, task_type, task_reference, "
          "description, task_config, retry_policy, position_x, position_y, "
          "metadata, COALESCE(priority, 0) as priority, "
          "COALESCE(condition_type, 'ALWAYS') as condition_type, "
          "condition_expression, parent_condition_task_name, "
          "loop_type, loop_config, created_at, updated_at "
          "FROM metadata.workflow_tasks WHERE workflow_name = $1"),
          taskParams);
      
      for (const auto &taskRow : taskResults) {
        workflow.tasks.push_back(rowToTask(taskRow));
      }
      
      pqxx::params depParams;
      depParams.append(workflow.workflow_name);
      auto depResults = txn.exec(
          pqxx::zview("SELECT id, workflow_name, upstream_task_name, downstream_task_name, "
          "dependency_type, condition_expression, created_at "
          "FROM metadata.workflow_dependencies WHERE workflow_name = $1"),
          depParams);
      
      for (const auto &depRow : depResults) {
        workflow.dependencies.push_back(rowToDependency(depRow));
      }
      
      workflows.push_back(workflow);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getActiveWorkflows",
                  "Error getting active workflows: " + std::string(e.what()));
  }
  return workflows;
}

WorkflowModel WorkflowRepository::getWorkflow(const std::string &workflowName) {
  WorkflowModel workflow;
  workflow.workflow_name = "";
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    pqxx::params workflowParams;
    workflowParams.append(workflowName);
    auto results = txn.exec(
        pqxx::zview("SELECT id, workflow_name, description, schedule_cron, active, enabled, "
        "retry_policy, sla_config, metadata, created_at, updated_at, "
        "last_execution_time, last_execution_status "
        "FROM metadata.workflows WHERE workflow_name = $1"),
        workflowParams);

    if (!results.empty()) {
      workflow = rowToWorkflow(results[0]);
      
      pqxx::params taskParams2;
      taskParams2.append(workflowName);
      auto taskResults = txn.exec(
          pqxx::zview("SELECT id, workflow_name, task_name, task_type, task_reference, "
          "description, task_config, retry_policy, position_x, position_y, "
          "metadata, created_at, updated_at "
          "FROM metadata.workflow_tasks WHERE workflow_name = $1"),
          taskParams2);
      
      for (const auto &taskRow : taskResults) {
        workflow.tasks.push_back(rowToTask(taskRow));
      }
      
      pqxx::params depParams2;
      depParams2.append(workflowName);
      auto depResults = txn.exec(
          pqxx::zview("SELECT id, workflow_name, upstream_task_name, downstream_task_name, "
          "dependency_type, condition_expression, created_at "
          "FROM metadata.workflow_dependencies WHERE workflow_name = $1"),
          depParams2);
      
      for (const auto &depRow : depResults) {
        workflow.dependencies.push_back(rowToDependency(depRow));
      }
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getWorkflow",
                  "Error getting workflow: " + std::string(e.what()));
  }
  return workflow;
}

void WorkflowRepository::insertOrUpdateWorkflow(const WorkflowModel &workflow) {
  if (workflow.workflow_name.empty()) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateWorkflow",
                  "Invalid input: workflow_name must not be empty");
    throw std::invalid_argument("workflow_name must not be empty");
  }
  
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    
    std::string retryPolicyStr = retryPolicyToJson(workflow.retry_policy).dump();
    std::string slaConfigStr = slaConfigToJson(workflow.sla_config).dump();
    std::string rollbackConfigStr = rollbackConfigToJson(workflow.rollback_config).dump();
    std::string metadataStr = workflow.metadata.dump();
    std::string scheduleCron = workflow.schedule_cron;
    
    pqxx::params existingParams;
    existingParams.append(workflow.workflow_name);
    auto existing = txn.exec(
        pqxx::zview("SELECT id FROM metadata.workflows WHERE workflow_name = $1"),
        existingParams);
    
    if (existing.empty()) {
      if (scheduleCron.empty()) {
        pqxx::params insertParams1;
        insertParams1.append(workflow.workflow_name);
        insertParams1.append(workflow.description);
        insertParams1.append(workflow.active);
        insertParams1.append(workflow.enabled);
        insertParams1.append(retryPolicyStr);
        insertParams1.append(slaConfigStr);
        insertParams1.append(metadataStr);
        txn.exec(
            pqxx::zview("INSERT INTO metadata.workflows (workflow_name, description, "
            "schedule_cron, active, enabled, retry_policy, sla_config, metadata) "
            "VALUES ($1, $2, NULL, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb)"),
            insertParams1);
      } else {
        pqxx::params insertParams2;
        insertParams2.append(workflow.workflow_name);
        insertParams2.append(workflow.description);
        insertParams2.append(scheduleCron);
        insertParams2.append(workflow.active);
        insertParams2.append(workflow.enabled);
        insertParams2.append(retryPolicyStr);
        insertParams2.append(slaConfigStr);
        insertParams2.append(metadataStr);
        txn.exec(
            pqxx::zview("INSERT INTO metadata.workflows (workflow_name, description, "
            "schedule_cron, active, enabled, retry_policy, sla_config, metadata) "
            "VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb)"),
            insertParams2);
      }
    } else {
      int id = existing[0][0].as<int>();
      if (scheduleCron.empty()) {
        pqxx::params updateParams1;
        updateParams1.append(id);
        updateParams1.append(workflow.description);
        updateParams1.append(workflow.active);
        updateParams1.append(workflow.enabled);
        updateParams1.append(retryPolicyStr);
        updateParams1.append(slaConfigStr);
        updateParams1.append(metadataStr);
        txn.exec(
            pqxx::zview("UPDATE metadata.workflows SET description = $2, schedule_cron = NULL, "
            "active = $3, enabled = $4, retry_policy = $5::jsonb, "
            "sla_config = $6::jsonb, metadata = $7::jsonb, updated_at = NOW() "
            "WHERE id = $1"),
            updateParams1);
      } else {
        pqxx::params updateParams2;
        updateParams2.append(id);
        updateParams2.append(workflow.description);
        updateParams2.append(scheduleCron);
        updateParams2.append(workflow.active);
        updateParams2.append(workflow.enabled);
        updateParams2.append(retryPolicyStr);
        updateParams2.append(slaConfigStr);
        updateParams2.append(rollbackConfigStr);
        updateParams2.append(metadataStr);
        txn.exec(
            pqxx::zview("UPDATE metadata.workflows SET description = $2, schedule_cron = $3, "
            "active = $4, enabled = $5, retry_policy = $6::jsonb, "
            "sla_config = $7::jsonb, rollback_config = $8::jsonb, metadata = $9::jsonb, updated_at = NOW() "
            "WHERE id = $1"),
            updateParams2);
      }
    }
    
    for (const auto &task : workflow.tasks) {
      std::string taskConfigStr = task.task_config.dump();
      std::string taskRetryPolicyStr = retryPolicyToJson(task.retry_policy).dump();
      std::string taskMetadataStr = task.metadata.dump();
      
      pqxx::params taskExistingParams;
      taskExistingParams.append(workflow.workflow_name);
      taskExistingParams.append(task.task_name);
      auto taskExisting = txn.exec(
          pqxx::zview("SELECT id FROM metadata.workflow_tasks WHERE workflow_name = $1 AND task_name = $2"),
          taskExistingParams);
      
      std::string loopConfigStr = task.loop_config.dump();
      std::string conditionTypeStr = conditionTypeToString(task.condition_type);
      std::string loopTypeStr = (task.loop_type == LoopType::FOR || task.loop_type == LoopType::WHILE || task.loop_type == LoopType::FOREACH) 
                                 ? loopTypeToString(task.loop_type) : "";
      
      if (taskExisting.empty()) {
        pqxx::params taskInsertParams;
        taskInsertParams.append(workflow.workflow_name);
        taskInsertParams.append(task.task_name);
        taskInsertParams.append(taskTypeToString(task.task_type));
        taskInsertParams.append(task.task_reference);
        taskInsertParams.append(task.description);
        taskInsertParams.append(taskConfigStr);
        taskInsertParams.append(taskRetryPolicyStr);
        taskInsertParams.append(task.position_x);
        taskInsertParams.append(task.position_y);
        taskInsertParams.append(taskMetadataStr);
        taskInsertParams.append(task.priority);
        taskInsertParams.append(conditionTypeStr);
        taskInsertParams.append(task.condition_expression);
        taskInsertParams.append(task.parent_condition_task_name);
        taskInsertParams.append(loopTypeStr.empty() ? pqxx::zview(nullptr) : pqxx::zview(loopTypeStr));
        taskInsertParams.append(loopConfigStr);
        txn.exec(
            pqxx::zview("INSERT INTO metadata.workflow_tasks (workflow_name, task_name, "
            "task_type, task_reference, description, task_config, retry_policy, "
            "position_x, position_y, metadata, priority, condition_type, "
            "condition_expression, parent_condition_task_name, loop_type, loop_config) "
            "VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10::jsonb, "
            "$11, $12, $13, $14, $15, $16::jsonb)"),
            taskInsertParams);
      } else {
        int taskId = taskExisting[0][0].as<int>();
        pqxx::params taskUpdateParams;
        taskUpdateParams.append(taskId);
        taskUpdateParams.append(workflow.workflow_name);
        taskUpdateParams.append(taskTypeToString(task.task_type));
        taskUpdateParams.append(task.task_reference);
        taskUpdateParams.append(task.description);
        taskUpdateParams.append(taskConfigStr);
        taskUpdateParams.append(taskRetryPolicyStr);
        taskUpdateParams.append(task.position_x);
        taskUpdateParams.append(task.position_y);
        taskUpdateParams.append(taskMetadataStr);
        taskUpdateParams.append(task.priority);
        taskUpdateParams.append(conditionTypeStr);
        taskUpdateParams.append(task.condition_expression);
        taskUpdateParams.append(task.parent_condition_task_name);
        taskUpdateParams.append(loopTypeStr.empty() ? pqxx::zview(nullptr) : pqxx::zview(loopTypeStr));
        taskUpdateParams.append(loopConfigStr);
        txn.exec(
            pqxx::zview("UPDATE metadata.workflow_tasks SET task_type = $3, task_reference = $4, "
            "description = $5, task_config = $6::jsonb, retry_policy = $7::jsonb, "
            "position_x = $8, position_y = $9, metadata = $10::jsonb, "
            "priority = $11, condition_type = $12, condition_expression = $13, "
            "parent_condition_task_name = $14, loop_type = $15, loop_config = $16::jsonb, updated_at = NOW() "
            "WHERE id = $1 AND workflow_name = $2"),
            taskUpdateParams);
      }
    }
    
    pqxx::params deleteDepParams;
    deleteDepParams.append(workflow.workflow_name);
    txn.exec(
        pqxx::zview("DELETE FROM metadata.workflow_dependencies WHERE workflow_name = $1"),
        deleteDepParams);
    
    for (const auto &dep : workflow.dependencies) {
      pqxx::params depInsertParams;
      depInsertParams.append(workflow.workflow_name);
      depInsertParams.append(dep.upstream_task_name);
      depInsertParams.append(dep.downstream_task_name);
      depInsertParams.append(dependencyTypeToString(dep.dependency_type));
      depInsertParams.append(dep.condition_expression);
      txn.exec(
          pqxx::zview("INSERT INTO metadata.workflow_dependencies (workflow_name, "
          "upstream_task_name, downstream_task_name, dependency_type, condition_expression) "
          "VALUES ($1, $2, $3, $4, $5)"),
          depInsertParams);
    }
    
    txn.commit();

    try {
      WorkflowVersionManager::getInstance().createVersion(
          workflow.workflow_name, "system", "Auto-save");
    } catch (const std::exception &e) {
      Logger::warning(LogCategory::DATABASE, "insertOrUpdateWorkflow",
                      "Could not create workflow version (non-fatal): " +
                          std::string(e.what()));
    }
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "insertOrUpdateWorkflow",
                  "Error inserting/updating workflow: " + std::string(e.what()));
    throw;
  }
}

void WorkflowRepository::deleteWorkflow(const std::string &workflowName) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    pqxx::params deleteParams;
    deleteParams.append(workflowName);
    txn.exec(pqxx::zview("DELETE FROM metadata.workflows WHERE workflow_name = $1"),
             deleteParams);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "deleteWorkflow",
                  "Error deleting workflow: " + std::string(e.what()));
    throw;
  }
}

void WorkflowRepository::updateWorkflowActive(const std::string &workflowName,
                                               bool active) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    pqxx::params activeParams;
    activeParams.append(active);
    activeParams.append(workflowName);
    txn.exec(
        pqxx::zview("UPDATE metadata.workflows SET active = $1, updated_at = NOW() "
        "WHERE workflow_name = $2"),
        activeParams);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateWorkflowActive",
                  "Error updating workflow active status: " + std::string(e.what()));
    throw;
  }
}

void WorkflowRepository::updateWorkflowEnabled(const std::string &workflowName,
                                                bool enabled) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    pqxx::params enabledParams;
    enabledParams.append(enabled);
    enabledParams.append(workflowName);
    txn.exec(
        pqxx::zview("UPDATE metadata.workflows SET enabled = $1, updated_at = NOW() "
        "WHERE workflow_name = $2"),
        enabledParams);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateWorkflowEnabled",
                  "Error updating workflow enabled status: " + std::string(e.what()));
    throw;
  }
}

void WorkflowRepository::updateLastExecution(const std::string &workflowName,
                                             const std::string &executionTime,
                                             const std::string &status) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    pqxx::params lastExecParams;
    lastExecParams.append(executionTime);
    lastExecParams.append(status);
    lastExecParams.append(workflowName);
    txn.exec(
        pqxx::zview("UPDATE metadata.workflows SET last_execution_time = $1::timestamp, "
        "last_execution_status = $2, updated_at = NOW() WHERE workflow_name = $3"),
        lastExecParams);
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateLastExecution",
                  "Error updating last execution: " + std::string(e.what()));
    throw;
  }
}

std::vector<WorkflowExecution>
WorkflowRepository::getWorkflowExecutions(const std::string &workflowName,
                                           int limit) {
  std::vector<WorkflowExecution> executions;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    pqxx::params execParams1;
    execParams1.append(workflowName);
    execParams1.append(limit);
    auto results = txn.exec(
        pqxx::zview("SELECT id, workflow_name, execution_id, status, trigger_type, "
        "start_time, end_time, duration_seconds, total_tasks, completed_tasks, "
        "failed_tasks, skipped_tasks, error_message, metadata, created_at "
        "FROM metadata.workflow_executions WHERE workflow_name = $1 "
        "ORDER BY created_at DESC LIMIT $2"),
        execParams1);

    for (const auto &row : results) {
      executions.push_back(rowToExecution(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getWorkflowExecutions",
                  "Error getting workflow executions: " + std::string(e.what()));
  }
  return executions;
}

WorkflowExecution
WorkflowRepository::getWorkflowExecution(const std::string &executionId) {
  WorkflowExecution execution;
  execution.execution_id = "";
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    pqxx::params execParams2;
    execParams2.append(executionId);
    auto results = txn.exec(
        pqxx::zview("SELECT id, workflow_name, execution_id, status, trigger_type, "
        "start_time, end_time, duration_seconds, total_tasks, completed_tasks, "
        "failed_tasks, skipped_tasks, error_message, metadata, created_at "
        "FROM metadata.workflow_executions WHERE execution_id = $1"),
        execParams2);

    if (!results.empty()) {
      execution = rowToExecution(results[0]);
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getWorkflowExecution",
                  "Error getting workflow execution: " + std::string(e.what()));
  }
  return execution;
}

int64_t WorkflowRepository::createWorkflowExecution(
    const WorkflowExecution &execution) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    
    std::string metadataStr = execution.metadata.dump();
    std::string startTime = execution.start_time;
    std::string endTime = execution.end_time;
    
    pqxx::params createExecParams;
    createExecParams.append(execution.workflow_name);
    createExecParams.append(execution.execution_id);
    createExecParams.append(executionStatusToString(execution.status));
    createExecParams.append(triggerTypeToString(execution.trigger_type));
    createExecParams.append(startTime);
    createExecParams.append(endTime);
    createExecParams.append(execution.duration_seconds);
    createExecParams.append(execution.total_tasks);
    createExecParams.append(execution.completed_tasks);
    createExecParams.append(execution.failed_tasks);
    createExecParams.append(execution.skipped_tasks);
    createExecParams.append(execution.error_message);
    createExecParams.append(rollbackStatusToString(execution.rollback_status));
    createExecParams.append(metadataStr);
    auto result = txn.exec(
        pqxx::zview("INSERT INTO metadata.workflow_executions (workflow_name, execution_id, "
        "status, trigger_type, start_time, end_time, duration_seconds, "
        "total_tasks, completed_tasks, failed_tasks, skipped_tasks, "
        "error_message, rollback_status, metadata) "
        "VALUES ($1, $2, $3, $4, $5::timestamp, $6::timestamp, $7, $8, $9, $10, "
        "$11, $12, $13, $14::jsonb) RETURNING id"),
        createExecParams);

    if (!result.empty()) {
      int64_t id = result[0][0].as<int64_t>();
      txn.commit();
      return id;
    }
    txn.commit();
    return 0;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "createWorkflowExecution",
                  "Error creating workflow execution: " + std::string(e.what()));
    throw;
  }
}

void WorkflowRepository::updateWorkflowExecution(
    const WorkflowExecution &execution) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    
    std::string metadataStr = execution.metadata.dump();
    std::string startTime = execution.start_time;
    std::string endTime = execution.end_time;
    
    pqxx::params updateExecParams;
    updateExecParams.append(execution.id);
    updateExecParams.append(executionStatusToString(execution.status));
    updateExecParams.append(startTime);
    updateExecParams.append(endTime);
    updateExecParams.append(execution.duration_seconds);
    updateExecParams.append(execution.total_tasks);
    updateExecParams.append(execution.completed_tasks);
    updateExecParams.append(execution.failed_tasks);
    updateExecParams.append(execution.skipped_tasks);
    updateExecParams.append(execution.error_message);
    updateExecParams.append(rollbackStatusToString(execution.rollback_status));
    updateExecParams.append(execution.rollback_started_at.empty() ? pqxx::zview(nullptr) : pqxx::zview(execution.rollback_started_at));
    updateExecParams.append(execution.rollback_completed_at.empty() ? pqxx::zview(nullptr) : pqxx::zview(execution.rollback_completed_at));
    updateExecParams.append(execution.rollback_error_message.empty() ? pqxx::zview(nullptr) : pqxx::zview(execution.rollback_error_message));
    updateExecParams.append(metadataStr);
    txn.exec(
        pqxx::zview("UPDATE metadata.workflow_executions SET status = $2, start_time = $3::timestamp, "
        "end_time = $4::timestamp, duration_seconds = $5, total_tasks = $6, "
        "completed_tasks = $7, failed_tasks = $8, skipped_tasks = $9, "
        "error_message = $10, rollback_status = $11, "
        "rollback_started_at = $12, rollback_completed_at = $13, rollback_error_message = $14, "
        "metadata = $15::jsonb WHERE id = $1"),
        updateExecParams);
    
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateWorkflowExecution",
                  "Error updating workflow execution: " + std::string(e.what()));
    throw;
  }
}

std::vector<TaskExecution>
WorkflowRepository::getTaskExecutions(int64_t workflowExecutionId) {
  std::vector<TaskExecution> executions;
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    pqxx::params taskExecParams1;
    taskExecParams1.append(workflowExecutionId);
    auto results = txn.exec(
        pqxx::zview("SELECT id, workflow_execution_id, workflow_name, task_name, status, "
        "start_time, end_time, duration_seconds, retry_count, error_message, "
        "task_output, metadata, created_at "
        "FROM metadata.workflow_task_executions WHERE workflow_execution_id = $1 "
        "ORDER BY created_at"),
        taskExecParams1);

    for (const auto &row : results) {
      executions.push_back(rowToTaskExecution(row));
    }
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "getTaskExecutions",
                  "Error getting task executions: " + std::string(e.what()));
  }
  return executions;
}

int64_t WorkflowRepository::createTaskExecution(const TaskExecution &execution) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    
    std::string taskOutputStr = execution.task_output.dump();
    std::string metadataStr = execution.metadata.dump();
    std::string startTime = execution.start_time;
    std::string endTime = execution.end_time;
    
    pqxx::params createTaskExecParams;
    createTaskExecParams.append(execution.workflow_execution_id);
    createTaskExecParams.append(execution.workflow_name);
    createTaskExecParams.append(execution.task_name);
    createTaskExecParams.append(executionStatusToString(execution.status));
    createTaskExecParams.append(startTime);
    createTaskExecParams.append(endTime);
    createTaskExecParams.append(execution.duration_seconds);
    createTaskExecParams.append(execution.retry_count);
    createTaskExecParams.append(execution.error_message);
    createTaskExecParams.append(taskOutputStr);
    createTaskExecParams.append(metadataStr);
    auto result = txn.exec(
        pqxx::zview("INSERT INTO metadata.workflow_task_executions (workflow_execution_id, "
        "workflow_name, task_name, status, start_time, end_time, duration_seconds, "
        "retry_count, error_message, task_output, metadata) "
        "VALUES ($1, $2, $3, $4, $5::timestamp, $6::timestamp, $7, $8, $9, "
        "$10::jsonb, $11::jsonb) RETURNING id"),
        createTaskExecParams);

    if (!result.empty()) {
      int64_t id = result[0][0].as<int64_t>();
      txn.commit();
      return id;
    }
    txn.commit();
    return 0;
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "createTaskExecution",
                  "Error creating task execution: " + std::string(e.what()));
    throw;
  }
}

void WorkflowRepository::updateTaskExecution(const TaskExecution &execution) {
  try {
    auto conn = getConnection();
    pqxx::work txn(conn);
    
    std::string taskOutputStr = execution.task_output.dump();
    std::string metadataStr = execution.metadata.dump();
    std::string startTime = execution.start_time;
    std::string endTime = execution.end_time;
    
    pqxx::params updateTaskExecParams;
    updateTaskExecParams.append(execution.id);
    updateTaskExecParams.append(executionStatusToString(execution.status));
    updateTaskExecParams.append(startTime);
    updateTaskExecParams.append(endTime);
    updateTaskExecParams.append(execution.duration_seconds);
    updateTaskExecParams.append(execution.retry_count);
    updateTaskExecParams.append(execution.error_message);
    updateTaskExecParams.append(taskOutputStr);
    updateTaskExecParams.append(metadataStr);
    txn.exec(
        pqxx::zview("UPDATE metadata.workflow_task_executions SET status = $2, "
        "start_time = $3::timestamp, end_time = $4::timestamp, "
        "duration_seconds = $5, retry_count = $6, error_message = $7, "
        "task_output = $8::jsonb, metadata = $9::jsonb WHERE id = $1"),
        updateTaskExecParams);
    
    txn.commit();
  } catch (const std::exception &e) {
    Logger::error(LogCategory::DATABASE, "updateTaskExecution",
                  "Error updating task execution: " + std::string(e.what()));
    throw;
  }
}

WorkflowModel WorkflowRepository::rowToWorkflow(const pqxx::row &row) {
  WorkflowModel workflow;
  workflow.id = row[0].as<int>();
  workflow.workflow_name = row[1].as<std::string>();
  workflow.description = row[2].is_null() ? "" : row[2].as<std::string>();
  workflow.schedule_cron = row[3].is_null() ? "" : row[3].as<std::string>();
  workflow.active = row[4].as<bool>();
  workflow.enabled = row[5].as<bool>();
  
  if (!row[6].is_null()) {
    try {
      workflow.retry_policy = parseRetryPolicy(json::parse(row[6].as<std::string>()));
    } catch (...) {
      workflow.retry_policy = RetryPolicy();
    }
  }
  
  if (!row[7].is_null()) {
    try {
      workflow.sla_config = parseSLAConfig(json::parse(row[7].as<std::string>()));
    } catch (...) {
      workflow.sla_config = SLAConfig();
    }
  }
  
  int colIdx = 8;
  if (row.size() > colIdx && !row[colIdx].is_null()) {
    try {
      workflow.rollback_config = parseRollbackConfig(json::parse(row[colIdx].as<std::string>()));
    } catch (...) {
      workflow.rollback_config = RollbackConfig();
    }
    colIdx++;
  }
  
  if (row.size() > colIdx && !row[colIdx].is_null()) {
    try {
      workflow.metadata = json::parse(row[colIdx].as<std::string>());
    } catch (...) {
      workflow.metadata = json{};
    }
  }
  
  workflow.created_at = row[9].is_null() ? "" : row[9].as<std::string>();
  workflow.updated_at = row[10].is_null() ? "" : row[10].as<std::string>();
  workflow.last_execution_time = row[11].is_null() ? "" : row[11].as<std::string>();
  workflow.last_execution_status = row[12].is_null() ? "" : row[12].as<std::string>();
  
  return workflow;
}

WorkflowTask WorkflowRepository::rowToTask(const pqxx::row &row) {
  WorkflowTask task;
  task.id = row[0].as<int>();
  task.workflow_name = row[1].as<std::string>();
  task.task_name = row[2].as<std::string>();
  task.task_type = stringToTaskType(row[3].as<std::string>());
  task.task_reference = row[4].as<std::string>();
  task.description = row[5].is_null() ? "" : row[5].as<std::string>();
  
  if (!row[6].is_null()) {
    try {
      task.task_config = json::parse(row[6].as<std::string>());
    } catch (...) {
      task.task_config = json{};
    }
  }
  
  if (!row[7].is_null()) {
    try {
      task.retry_policy = parseRetryPolicy(json::parse(row[7].as<std::string>()));
    } catch (...) {
      task.retry_policy = RetryPolicy();
    }
  }
  
  task.position_x = row[8].as<int>();
  task.position_y = row[9].as<int>();
  
  if (!row[10].is_null()) {
    try {
      task.metadata = json::parse(row[10].as<std::string>());
    } catch (...) {
      task.metadata = json{};
    }
  }
  
  int colIdx = 11;
  if (row.size() > colIdx) {
    task.priority = row[colIdx].is_null() ? 0 : row[colIdx].as<int>();
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    std::string conditionTypeStr = row[colIdx].is_null() ? "ALWAYS" : row[colIdx].as<std::string>();
    if (conditionTypeStr == "IF") task.condition_type = ConditionType::IF;
    else if (conditionTypeStr == "ELSE") task.condition_type = ConditionType::ELSE;
    else if (conditionTypeStr == "ELSE_IF") task.condition_type = ConditionType::ELSE_IF;
    else task.condition_type = ConditionType::ALWAYS;
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    task.condition_expression = row[colIdx].is_null() ? "" : row[colIdx].as<std::string>();
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    task.parent_condition_task_name = row[colIdx].is_null() ? "" : row[colIdx].as<std::string>();
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    std::string loopTypeStr = row[colIdx].is_null() ? "" : row[colIdx].as<std::string>();
    if (loopTypeStr == "FOR") task.loop_type = LoopType::FOR;
    else if (loopTypeStr == "WHILE") task.loop_type = LoopType::WHILE;
    else if (loopTypeStr == "FOREACH") task.loop_type = LoopType::FOREACH;
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    if (!row[colIdx].is_null()) {
      try {
        task.loop_config = json::parse(row[colIdx].as<std::string>());
      } catch (...) {
        task.loop_config = json{};
      }
    }
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    task.created_at = row[colIdx].is_null() ? "" : row[colIdx].as<std::string>();
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    task.updated_at = row[colIdx].is_null() ? "" : row[colIdx].as<std::string>();
  }
  
  return task;
}

WorkflowDependency WorkflowRepository::rowToDependency(const pqxx::row &row) {
  WorkflowDependency dep;
  dep.id = row[0].as<int>();
  dep.workflow_name = row[1].as<std::string>();
  dep.upstream_task_name = row[2].as<std::string>();
  dep.downstream_task_name = row[3].as<std::string>();
  dep.dependency_type = stringToDependencyType(row[4].as<std::string>());
  dep.condition_expression = row[5].is_null() ? "" : row[5].as<std::string>();
  dep.created_at = row[6].is_null() ? "" : row[6].as<std::string>();
  return dep;
}

WorkflowExecution WorkflowRepository::rowToExecution(const pqxx::row &row) {
  WorkflowExecution execution;
  execution.id = row[0].as<int64_t>();
  execution.workflow_name = row[1].as<std::string>();
  execution.execution_id = row[2].as<std::string>();
  execution.status = stringToExecutionStatus(row[3].as<std::string>());
  execution.trigger_type = stringToTriggerType(row[4].as<std::string>());
  execution.start_time = row[5].is_null() ? "" : row[5].as<std::string>();
  execution.end_time = row[6].is_null() ? "" : row[6].as<std::string>();
  execution.duration_seconds = row[7].is_null() ? 0 : row[7].as<int>();
  execution.total_tasks = row[8].is_null() ? 0 : row[8].as<int>();
  execution.completed_tasks = row[9].is_null() ? 0 : row[9].as<int>();
  execution.failed_tasks = row[10].is_null() ? 0 : row[10].as<int>();
  execution.skipped_tasks = row[11].is_null() ? 0 : row[11].as<int>();
  execution.error_message = row[12].is_null() ? "" : row[12].as<std::string>();
  
  int colIdx = 13;
  if (row.size() > colIdx) {
    std::string rollbackStatusStr = row[colIdx].is_null() ? "" : row[colIdx].as<std::string>();
    if (rollbackStatusStr == "IN_PROGRESS") execution.rollback_status = RollbackStatus::IN_PROGRESS;
    else if (rollbackStatusStr == "COMPLETED") execution.rollback_status = RollbackStatus::COMPLETED;
    else if (rollbackStatusStr == "FAILED") execution.rollback_status = RollbackStatus::FAILED;
    else execution.rollback_status = RollbackStatus::PENDING;
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    execution.rollback_started_at = row[colIdx].is_null() ? "" : row[colIdx].as<std::string>();
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    execution.rollback_completed_at = row[colIdx].is_null() ? "" : row[colIdx].as<std::string>();
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    execution.rollback_error_message = row[colIdx].is_null() ? "" : row[colIdx].as<std::string>();
    colIdx++;
  }
  
  if (row.size() > colIdx && !row[colIdx].is_null()) {
    try {
      execution.metadata = json::parse(row[colIdx].as<std::string>());
    } catch (...) {
      execution.metadata = json{};
    }
    colIdx++;
  }
  
  if (row.size() > colIdx) {
    execution.created_at = row[colIdx].is_null() ? "" : row[colIdx].as<std::string>();
  }
  
  return execution;
}

TaskExecution WorkflowRepository::rowToTaskExecution(const pqxx::row &row) {
  TaskExecution execution;
  execution.id = row[0].as<int64_t>();
  execution.workflow_execution_id = row[1].as<int64_t>();
  execution.workflow_name = row[2].as<std::string>();
  execution.task_name = row[3].as<std::string>();
  execution.status = stringToExecutionStatus(row[4].as<std::string>());
  execution.start_time = row[5].is_null() ? "" : row[5].as<std::string>();
  execution.end_time = row[6].is_null() ? "" : row[6].as<std::string>();
  execution.duration_seconds = row[7].is_null() ? 0 : row[7].as<int>();
  execution.retry_count = row[8].is_null() ? 0 : row[8].as<int>();
  execution.error_message = row[9].is_null() ? "" : row[9].as<std::string>();
  
  if (!row[10].is_null()) {
    try {
      execution.task_output = json::parse(row[10].as<std::string>());
    } catch (...) {
      execution.task_output = json{};
    }
  }
  
  if (!row[11].is_null()) {
    try {
      execution.metadata = json::parse(row[11].as<std::string>());
    } catch (...) {
      execution.metadata = json{};
    }
  }
  
  execution.created_at = row[12].is_null() ? "" : row[12].as<std::string>();
  return execution;
}

std::string WorkflowRepository::conditionTypeToString(ConditionType type) {
  switch (type) {
    case ConditionType::IF: return "IF";
    case ConditionType::ELSE: return "ELSE";
    case ConditionType::ELSE_IF: return "ELSE_IF";
    default: return "ALWAYS";
  }
}

ConditionType WorkflowRepository::stringToConditionType(const std::string &str) {
  if (str == "IF") return ConditionType::IF;
  if (str == "ELSE") return ConditionType::ELSE;
  if (str == "ELSE_IF") return ConditionType::ELSE_IF;
  return ConditionType::ALWAYS;
}

std::string WorkflowRepository::loopTypeToString(LoopType type) {
  switch (type) {
    case LoopType::FOR: return "FOR";
    case LoopType::WHILE: return "WHILE";
    case LoopType::FOREACH: return "FOREACH";
  }
  return "";
}

LoopType WorkflowRepository::stringToLoopType(const std::string &str) {
  if (str == "FOR") return LoopType::FOR;
  if (str == "WHILE") return LoopType::WHILE;
  if (str == "FOREACH") return LoopType::FOREACH;
  return LoopType::FOR;
}
