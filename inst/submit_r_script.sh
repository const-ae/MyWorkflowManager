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

module load R-bundle-Bioconductor/3.16-foss-2022b-R-4.2.2
STAT_FILE=$1
shift
/usr/bin/time --output=$STAT_FILE --format="elapsed %e\nuser %U\nsys %S\nmax_mem_kbytes %M\n" Rscript --no-restore $@
exit_if_error $? "Rscript"


echo "Finished Script"
