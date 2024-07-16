#!/bin/sh

unset source_dir
unset target_dir

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
      echo "  --source-dir S  Path to local resource directory, for example"
      echo "                  'resources'"
      echo "  --target-dir T  Path workspace folder for resources, for example"
      echo "                  'dbfs:/FileStore/dfs-aa-dl/dbutils'"
      exit 0
      ;;
    --source-dir)
      shift
      source_dir="$1"
      echo "Source directory: $source_dir"
      shift
      ;;
    --target-dir)
      shift
      target_dir="$1"
      echo "Target directory: $target_dir"
      shift
      ;;
    *)
      error_exit "Unknown option provided: $1"
  esac
done

if [ -z "$source_dir" ]
then
  error_exit "No source directory provided"
fi

if [ -z "$target_dir" ]
then
  error_exit "No target directory provided"
fi

export main_notebook resource_dir

# Create/clean target folder
databricks fs mkdirs "$target_dir" || exit 1
databricks fs rm -r "$target_dir" || exit 1
databricks fs mkdirs "$target_dir" || exit 1

# Copy resources to target folder
databricks fs cp -r "$source_dir" "$target_dir" || exit 1
