#!/bin/bash

# error handler function, modified from
# https://stackoverflow.com/a/50265513

exit_if_error() {
  local exit_code=$1
  shift
  [[ $exit_code ]] &&               # do nothing if no error code passed
    ((exit_code != 0)) && {         # do nothing if error code is 0
      printf 'ERROR: %s\n' "$@" >&2 # we can use better logging here
      exit "$exit_code"
    }
    printf '%s finished succesfully\n' "$@" # success message
}

echo "Start Script"

STAT_FILE=$1
shift

module purge
module load Anaconda3/2023.03-1
module load CUDA/11.7.0
source /g/easybuild/x86_64/Rocky/8/haswell/software/Anaconda3/2023.03-1/bin/activate $1
shift

/usr/bin/time --output=$STAT_FILE --format="elapsed %e\nuser %U\nsys %S\nmax_mem_kbytes %M\n" python $@
exit_if_error $? "python"

echo "Finished Script"
