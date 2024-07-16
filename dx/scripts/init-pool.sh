#!/bin/sh

export min_idle_instances="0"
export node_type_id="Standard_DS3_v2"
export idle_instance_autotermination_minutes="60"
export preloaded_spark_version="7.3.x-scala2.12"
export instance_pool_name=""

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
      echo "Creates or updates an instance pool, and returns its id."
      echo
      echo "Options:"
      echo "  --name N             The name of the instance pool."
      echo "                       Default: Formed from node type and runtime version."
      echo "  --idle N             The minimum number of idle instances maintained by the pool."
      echo "                       Default: $min_idle_instances"
      echo "  --node-type N        The node type for the instances in the pool."
      echo "                       Default: $node_type_id"
      echo "  --autotermination N  The number of minutes that idle instances in excess of the"  
      echo "                       minimum are maintained by the pool before being terminated."
      echo "                       Default: $idle_instance_autotermination_minutes"
      echo "  --spark-version N    Runtime version the pool installs on each instance."
      echo "                       Default: $preloaded_spark_version"
      echo "                       Provide '' to use proloaded runtimes."
      exit 0;;
    --name)            instance_pool_name="$2";                    shift 2;;
    --idle)            min_idle_instances="$2";                    shift 2;;
    --node-type)       node_type_id="$2";                          shift 2;;
    --autotermination) idle_instance_autotermination_minutes="$2"; shift 2;;
    --spark-version)   preloaded_spark_version="$2";               shift 2;;
    *)                 error_exit "Unknown option provided: $1"
  esac
done

if [ -z "$instance_pool_name" ]
then
  instance_pool_name="$node_type_id $preloaded_spark_version"
fi

list="$(databricks instance-pools list --output JSON)" || error_exit "$list"
pool="$(echo "$list" | jq '.instance_pools[] | select(.instance_pool_name == env.instance_pool_name)')"

if [ -z "$pool" ]
then
  json="$(jq -n '{
    instance_pool_name: env.instance_pool_name,
    min_idle_instances: env.min_idle_instances | tonumber,
    node_type_id: env.node_type_id,
    idle_instance_autotermination_minutes: env.idle_instance_autotermination_minutes | tonumber,
    preloaded_spark_versions: [env.preloaded_spark_version | select(. != "")],
  }')"
  pool="$(databricks instance-pools create --json "$json")" || error_exit "$pool"
else
  json="$(echo "$pool" | jq '{
    instance_pool_name: env.instance_pool_name,
    min_idle_instances: env.min_idle_instances | tonumber,
    node_type_id: env.node_type_id,
    idle_instance_autotermination_minutes: env.idle_instance_autotermination_minutes | tonumber,
    instance_pool_id: .instance_pool_id,
  }')"
  edit="$(databricks instance-pools edit --json "$json")" || error_exit "$edit"
fi

instance_pool_id="$(echo "$pool" | jq -r '.instance_pool_id')"

if [ -z "$instance_pool_id" ]
then
  error_exit "Cannot determine instance pool id"
fi

echo "$instance_pool_id"
