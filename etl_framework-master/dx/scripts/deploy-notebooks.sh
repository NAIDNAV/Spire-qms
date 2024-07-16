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
      echo "  --source-dir S  Path to local notebook directory, for example"
      echo "                  'notebooks'"
      echo "  --target-dir T  Path workspace folder for notebooks, for example"
      echo "                  '/Code/dfs-aa-dl/dbutils'"
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

# Create/clean workspace folder
databricks workspace mkdirs "$target_dir" || exit 1
databricks workspace rm -r "$target_dir" || exit 1
databricks workspace mkdirs "$target_dir" || exit 1

# Import notebooks to workspace folder
databricks workspace import_dir "$source_dir" "$target_dir" || exit 1
