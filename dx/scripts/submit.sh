#!/bin/sh

export main_notebook=""
export spark_version="7.3.x-scala2.12"
export num_workers="1"
export node_type_id="Standard_DS3_v2"
export driver_node_type_id="Standard_DS3_v2"
export jars=""
export whls=""
export base_parameters=""
export instance_pool_id=""
export init_scripts=""

error_exit() {
  echo "$1" 1>&2
  exit 1
}

while [ $# -gt 0 ]
do
  case "$1"
  in
    --help|-h)
      echo "Usage: $(basename "$0") OPTIONS"
      echo
      echo "Options:"
      echo "  --main-notebook N  MANDATORY path to main notebook, for example"
      echo "                     '/Code/dfs-aa-dl/spire_template_onboarding/test'"
      echo "  --spark-version V  Databrick runtime specification."
      echo "                     Default: $spark_version"
      echo "  --num-workers N    Number of workers."
      echo "                     Default: $num_workers"
      echo "  --node-type-id N   Virtual machine type for workers."
      echo "                     Default: $node_type_id"
      echo "  --driver-node-type-id N"
      echo "                     Virtual machine type for driver."
      echo "                     Default: $driver_node_type_id"
      echo "  --jar J            Add jar library to cluster.  Can be used multiple times."
      echo "                     Example:"
      echo "                     \"dbfs:/FileStore/\$BUILD_REPOSITORY_NAME/libraries/spireutils_2.12-0.6.0.jar\""
      echo "  --whl W            Add whl library to cluster.  Can be used multiple times."
      echo "                     Example:"
      echo "                     \"dbfs:/FileStore/\$BUILD_REPOSITORY_NAME/libraries/spireutils-0.6.0-py3-none-any.whl\""
      echo "  --parameter P      Set a parameter.  Can be used multiple times."
      echo "                     The format is 'name=value'.  Must not contain ','."
      echo "                     Example: \"resourcePath=/FileStore/\$BUILD_REPOSITORY_NAME\""
      echo "  --instance-pool-id I"
      echo "                     Use the given instance pool. If this parameter is supplied,"
      echo "                     --driver-node-type-id and --nodetyp-id are ignored."
      echo "                     Example: 0717-131242-yeast336-pool-WLYqtl4Q"
      echo "  --init-script S    Add init script to cluster.  Can be used multiple times."
      echo "                     Example:"
      echo "                     \"dbfs:/FileStore/\$BUILD_REPOSITORY_NAME/initscripts/install-packages.sh\""
      exit 0
      ;;
    --main-notebook)
      shift
      main_notebook="$1"
      shift
      ;;
    --spark-version)
      shift
      spark_version="$1"
      shift
      ;;
    --num-workers)
      shift
      num_workers="$1"
      shift
      ;;
    --node-type-id)
      shift
      node_type_id="$1"
      shift
      ;;
    --driver-node-type-id)
      shift
      driver_node_type_id="$1"
      shift
      ;;
    --jar)
      shift
      if [ -z "$jars" ]
      then
        jars="$1"
      else
        jars="$jars,$1"
      fi
      shift
      ;;
    --whl)
      shift
      if [ -z "$whls" ]
      then
        whls="$1"
      else
        whls="$whls,$1"
      fi
      shift
      ;;
    --parameter)
      shift
      if [ -z "$base_parameters" ]
      then
        base_parameters="$1"
      else
        base_parameters="$base_parameters,$1"
      fi
      shift
      ;;
    --instance-pool-id)
      shift
      instance_pool_id="$1"
      shift
      ;;
    --init-script)
      shift
      if [ -z "$init_scripts" ]
      then
        init_scripts="$1"
      else
        init_scripts="$init_scripts,$1"
      fi
      shift
      ;;
    *)
      error_exit "Unknown option provided: $1"
  esac
done

if [ -z "$main_notebook" ]
then
  error_exit "No --main-notebook provided"
fi

echo "Main notebook: $main_notebook"
echo "Spark version: $spark_version"
echo "Number of workers: $num_workers"
echo "Node type id: $node_type_id"
echo "Driver node type id: $driver_node_type_id"
echo "Jars: $jars"
echo "Whls: $whls"
echo "Parameters: $base_parameters"
echo "Instance pool id: $instance_pool_id"
echo "Init scripts: $init_scripts"

if [ -n "$instance_pool_id" ]
then
  new_cluster="$(jq -n '{
    num_workers: env.num_workers | tonumber,
    spark_version: env.spark_version,
    instance_pool_id: env.instance_pool_id,
    spark_conf: {
      "spark.driver.extraJavaOptions": "-Dlog4j2.formatMsgNoLookups=true",
      "spark.executor.extraJavaOptions": "-Dlog4j2.formatMsgNoLookups=true"
    },
    init_scripts: env.init_scripts | split(",") | [{dbfs: {destination: .[]}}],
    cluster_log_conf: {
      dbfs: { destination: "dbfs:/cluster_logs" }
    },
    spark_env_vars: []
  }')"
else
  new_cluster="$(jq -n '{
    num_workers: env.num_workers | tonumber,
    spark_version: env.spark_version,
    spark_conf: {
      "spark.driver.extraJavaOptions": "-Dlog4j2.formatMsgNoLookups=true",
      "spark.executor.extraJavaOptions": "-Dlog4j2.formatMsgNoLookups=true"
    },
    node_type_id: env.node_type_id, 
    driver_node_type_id: env.driver_node_type_id,
    init_scripts: env.init_scripts | split(",") | [{dbfs: {destination: .[]}}],
    cluster_log_conf: {
      dbfs: { destination: "dbfs:/cluster_logs" }
    },
    spark_env_vars: []
  }')"
fi

export new_cluster

json="$(jq -n '{
  new_cluster: env.new_cluster | fromjson,
  notebook_task: {
    notebook_path: env.main_notebook,
    base_parameters:
      env.base_parameters | split(",") | map(
        split("=") | {(.[0]): .[1]}
      ) | add
  },
  libraries: [
    (env.jars | split(",") | {jar: .[]}),
    (env.whls | split(",") | {whl: .[]}) 
  ]
}')"

runs_submit="$(databricks runs submit --version 2.1 --json "$json")" || error_exit "$runs_submit"
run_id="$(echo "$runs_submit" | jq .run_id)" || error_exit "$runs_submit"
echo Run id: "$run_id"

while
  runs_get="$(databricks runs get --version 2.1 --run-id "$run_id")" || error_exit "$runs_get"
  life_cycle_state="$(echo "$runs_get" | jq -r .state.life_cycle_state)" || error_exit "$runs_get"
  echo Life cycle state: "$life_cycle_state"
  [ "$life_cycle_state" = PENDING ] || [ "$life_cycle_state" = RUNNING ] || [ "$life_cycle_state" = TERMINATING ]
do
  sleep 10
done

echo "$runs_get"

result_state="$(echo "$runs_get" | jq -r .state.result_state)"
echo Result state: "$result_state"

run_page_url="$(echo "$runs_get" | jq -r .run_page_url)"
echo Run page url: "$run_page_url"

runs_get_output="$(databricks runs get-output --version 2.1 --run-id "$run_id")" || error_exit "$runs_get_output"
result="$(echo "$runs_get_output" | jq -r '.notebook_output.result')" || error_exit "$runs_get_output"
echo "Result: $result"

if [ "$result_state" != SUCCESS ]
then
  exit 1
fi
