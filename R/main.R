
#'
#' @importFrom magrittr `%>%`
NULL

.global_state <- new.env(parent = emptyenv())
.global_state$output_folder <- NULL
.global_state$my_db_con <- NULL


.OUTPUT_FOLDER <- function(){
  out <- .global_state$output_folder
  if(is.null(out) || out == ""){
    stop("Please call 'init()' and set the output folder")
  }
  out
}

.DB_CONNECTION <- function(){
  con <- .global_state$my_db_con
  if(is.null(con)){
    stop("Please call 'init()' to initialize the database connection")
  }
  con
}



#' Create required subfolders
#'
#' @export
init <- function(output_folder){
  message("Using ", output_folder)

  if(! file.exists(output_folder)){
    stop("The output folder does not exist. Please provide a valid 'output_folder'")
  }

  if(! file.exists(file.path(output_folder, "results")))  dir.create(file.path(output_folder, "results"))
  if(! file.exists(file.path(output_folder, "params")))  dir.create(file.path(output_folder, "params"))
  if(! file.exists(file.path(output_folder, "scripts")))  dir.create(file.path(output_folder, "scripts"))
  if(! file.exists(file.path(output_folder, "logs")))  dir.create(file.path(output_folder, "logs"))
  if(! file.exists(file.path(output_folder, "stats")))  dir.create(file.path(output_folder, "stats"))
  if(! file.exists(file.path(output_folder, "slurm_job_overview")))  dir.create(file.path(output_folder, "slurm_job_overview"))

  .global_state$my_db_con <- RSQLite::dbConnect(RSQLite::SQLite(), dbname = file.path(output_folder, "./job_db.sqlite"), extended_types = TRUE)
  if(! RSQLite::dbExistsTable(.DB_CONNECTION(), "job_overview")){
    RSQLite::dbWriteTable(.DB_CONNECTION(), "job_overview", make_db_rows(make_empty = TRUE))
  }

  .global_state$output_folder <- output_folder
}


get_script_id <- function(script){
  # The chance of a hash collision follows the birthday paradox principle
  # For 'd = #number of unique hashes' and 'n = #scripts' or 'n = #params',
  # the chance of a collision is approx p = 1 - exp(-n^2 / (2 * d))
  # For n = 1e6 and d = 16^13, p = 0.01%
  readr::read_lines(script) %>%
    paste0(collapse = "\n") %>%
    digest::digest() %>%
    stringr::str_sub(end = 13)
}

store_script <- function(script){
  stopifnot(file.exists(script))
  script_id <- get_script_id(script)
  file_path <- file.path(.OUTPUT_FOLDER(), "scripts", script_id)
  # Check if script already exists
  if(file.exists(file_path)){
    # Do nothing
  }else{
    file.copy(script, to = file_path)
  }
  script_id
}

convert_to_string <- function(obj){
  stopifnot(is.list(obj))
  stopifnot(! is.null(names(obj)))
  str <- ""
  for(n in names(obj)){
    elem <- obj[[n]]
    stopifnot(is.character(elem) || is.numeric(elem))
    elem_str <- paste0(elem, collapse = " ")
    str <- paste0(str, " --", n, " ", elem_str)
  }
  str
}

store_params <- function(params){
  params_id <- digest::digest(params) %>%
    stringr::str_sub(end = 13)

  file_path <- file.path(.OUTPUT_FOLDER(), "params", params_id)
  # Check if script already exists
  if(file.exists(file_path)){
    # Do nothing
  }else{
    yaml::write_yaml(params, file = file_path)
  }
  params_id
}

#' Get the parameters for a job
#'
#' @param job a job object
#'
#' @export
get_params <- function(job){
  stopifnot(is.job(job))
  yaml::read_yaml(file.path(.OUTPUT_FOLDER(), "params", job$params_id))
}

get_slurm_id <- function(job){
  stopifnot(is.job(job))
  get_slurm_id_from_result_id(job$result_id)
}

#' Get the output path for the log file of a job
#'
#' @param job a job object
#'
#' @export
output_log_file_path <- function(job){
  slurm_id <- get_slurm_id(job)
  file.path(.OUTPUT_FOLDER(), "logs", paste0("id_", job$script_id, "-", job$params_id, "-slurmid_", slurm_id, ".log"))
}

#' @rdname output_log_file_path
#' @export
get_output_log <- function(job){
  file_path <- output_log_file_path(job)
  paste0(paste0(readr::read_lines(file_path), collapse = "\n"), "\n")
}

#' @rdname output_log_file_path
#' @export
show_output_log <- function(job, print = FALSE){
  file_path <- output_log_file_path(job)
  if(! print && interactive()){
    file.show(file_path)
  }else{
    cat(get_output_log(job))
  }
}


#' Get the output path for the result file of a job
#'
#' @param job a job object
#'
#' @export
result_file_path <- function(job){
  file.path(.OUTPUT_FOLDER(), "results", job$result_id)
}

#' @rdname result_file_path
#' @export
show_result_file <- function(job, print = FALSE){
  file_path <- result_file_path(job)
  if(! print && interactive()){
    file.show(file_path)
  }else{
    cat(paste0(paste0(readr::read_lines(file_path), collapse = "\n"), "\n"))
  }
}

#' Get the output path for the stats file of a job
#'
#' @param job a job object
#'
#' @export
stats_file_path <- function(job){
  file.path(.OUTPUT_FOLDER(), "stats", job$result_id)
}

#' @rdname stats_file_path
#' @export
show_stats_file <- function(job, print = FALSE){
  file_path <- stats_file_path(job)
  if(! print && interactive()){
    file.show(file_path)
  }else{
    cat(paste0(paste0(readr::read_lines(file_path), collapse = "\n"), "\n"))
  }
}

is_script_outdated <- function(job, script){
  job$script_id == get_script_id(script)
}

get_slurm_id_from_result_id <- function(result_id){
  file <- file.path(.OUTPUT_FOLDER(), "slurm_job_overview", result_id)
  if(file.exists(file.path(.OUTPUT_FOLDER(), "slurm_job_overview", result_id))){
    readr::read_lines(file)
  }else{
    stop("slurm id for result ", result_id, " not available")
  }
}

#' Slurm job status for a job
#'
#' Typically either "PENDING", "RUNNING", "FAILED", "CANCELLED", or "COMPLETED"
#' but there are several more options (see https://slurm.schedmd.com/sacct.html#SECTION_JOB-STATE-CODES)
#'
#' @export
slurm_job_status <- function(job){
  stopifnot(is.job(job))
  # get the 200 characters of the state variable left justified
  cmd_slurm_status <- glue::glue("sacct --jobs={get_slurm_id(job)} --format=state%-200 --noheader")
  stringr::str_trim(system(cmd_slurm_status, intern = TRUE)[1]) %>%
    stringr::str_split("\\s") %>% magrittr::extract2(1) %>% magrittr::extract(1)
}

slurm_status <- function(slurm_ids){
  # get the 200 characters of the state variable left justified
  cmd_slurm_status <- glue::glue("sacct --jobs={paste0(slurm_ids, collapse=",")} --format=state%-200 --noheader")
  # stringr::str_trim(system(cmd_slurm_status, intern = TRUE)[1]) %>%
  #   stringr::str_split("\\s") %>% magrittr::extract2(1) %>% magrittr::extract(1)
  system(cmd_slurm_status, intern = TRUE) %>%
    stringr::str_trim()
}

#' Slurm job info
#'
#' @export
slurm_job_info <- function(job){
  stopifnot(is.job(job))
  cmd_slurm_status <- glue::glue("sacct --jobs={get_slurm_id(job)}")
  cat(paste0(system(cmd_slurm_status, intern = TRUE), collapse = "\n"))
}

#' The job status using a defined vocabulary
#'
#'
#' @return Either "done", "running", "failed", or "not_stated"
#'
#' @export
job_status <- function(job){
  stopifnot(is.job(job))
  if(file.exists(file.path(.OUTPUT_FOLDER(), "slurm_job_overview/", job$result_id))){
    if(file.exists(file.path(.OUTPUT_FOLDER(), "results/", job$result_id))){
      "done"
    }else{
      slurm_status <- slurm_job_status(job)
      res <- switch (slurm_status,
                     "PENDING" = "running",
                     "RUNNING" = "running",
                     "SUSPENDED" = "running",
                     `NA` = "running",
                     "FAILED" = "failed",
                     "BOOT_FAIL" = "failed",
                     "CANCELLED" = "failed",
                     "TIMEOUT" = "failed",
                     "DEADLINE" = "failed",
                     "NODE_FAIL" = "failed",
                     "OUT_OF_MEMORY" = "failed",
                     "PREEMPTED" = "failed",
                     "COMPLETED" = "failed",
                     "other"
      )
      if(res == "other") stop("Unkown slurm job status: ", slurm_status)
      res
    }
  }else{
    "not_started"
  }
}

#' @export
is.job <- function(x) inherits(x, "job")

#' Wrap a computation described by a script and some input parameters
#'
#'
#' @param script the file path to the script
#' @param params a list of simple parameters that can be converted to command
#'   line options
#' @param dependencies a list of jobs whose results this job depends on
#' @param executor is it an R or a python script
#' @param extra_args a hack to specify the name of the conda environment that python loads
#' @param duration,memory,n_cpus the job resources
#'
#' @export
wrap_script <- function(script, params = list(), dependencies = list(),
                        executor = c("R", "python"), extra_args = "",
                        extra_slurm_args = "", duration = "01:00", memory = "1GB", n_cpus = 1){
  stopifnot(is.character(script))
  stopifnot(length(script) == 1L)
  stopifnot(all(vapply(dependencies, is.job, logical(1L))))
  stopifnot(is.list(params))
  stopifnot(! is.null(names(params)))
  stopifnot(all(vapply(params, function(elem) is.character(elem) || is.numeric(elem), logical(1L))))
  executor <- match.arg(executor)
  stopifnot(n_cpus %% 1 == 0)
  stopifnot(n_cpus > 0 && n_cpus <= 100)
  n_cpus <- as.character(n_cpus)

  script_extension <- tools::file_ext(script)
  if((script_extension == "R" && executor != "R") ||
     (script_extension == "py" && executor != "python")){
    stop("The file extension of the script and the executor don't match.")
  }

  script_id <- store_script(script)
  params_id <- store_params(params)

  result_id <- paste0(script_id, "-", params_id)

  res <- list(script_path = script, script_id = script_id, params_id = params_id, result_id = result_id,
              resources = list(duration = duration, memory = memory, n_cpus = n_cpus), executor = executor,
              extra_args = extra_args, extra_slurm_args = extra_slurm_args,
              dependencies = dependencies)
  class(res) <- "job"
  res
}


#' Run the job
#'
#'
#' @export
run_job <- function(job, priority = c("low", "normal", "high")){
  priority <- match.arg(priority)
  if(job_status(job) %in% c("not_started", "failed")){
    parameter_string <- convert_to_string(get_params(job))

    job_stats <- vapply(job$dependencies, function(e) job_status(e), character(1L))

    # Start jobs on which this job depends
    slurm_dependencies <- if(any(job_stats != "done")){
      for(dep in job$dependencies[job_stats != "done"]){
        run_job(dep, priority = priority)
      }
      paste0("--dependency=afterok:", paste0(vapply(job$dependencies[job_stats != "done"], get_slurm_id, character(1L)), collapse = ","))
    }else{
      if(sum(job_stats == "done") > 0){
        message("Not launching ", sum(job_stats == "done"), " which are already done.")
      }
      ""
    }

    submission <- glue::glue(
        r"(RES=$(sbatch --time {job$resources$duration} \
          --mem={job$resources$memory} \
          --export=None \
          -n {job$resources$n_cpus} \
          -N 1 \
          {job$extra_slurm_args} \
          --qos={priority} \
          {slurm_dependencies} \
          --parsable \
          -e {paste0(.OUTPUT_FOLDER(), "/logs/id_", job$result_id)}-slurmid_%j.log \
          -o {paste0(.OUTPUT_FOLDER(), "/logs/id_", job$result_id)}-slurmid_%j.log \
          {system.file(if(job$executor == "R") "submit_r_script.sh" else "submit_py_script.sh", package = "MyWorkflowManager")} \
            {paste0(.OUTPUT_FOLDER(), "/stats/", job$result_id)} {job$extra_args} \
              "{paste0(.OUTPUT_FOLDER(), "/scripts/", job$script_id)} {parameter_string} \
                --working_dir {.OUTPUT_FOLDER()} --result_id {job$result_id}") && \
          echo $RES > {file.path(.OUTPUT_FOLDER(), "slurm_job_overview", job$result_id)}
      )") %>% glue::trim()

    # submit job
    message("Launching new job")
    message(submission)
    system(submission, wait = TRUE)
    store_jobs(list(job))
  }else{
    message("Job is already running: ", job_status(job))
  }
  invisible(job)
}


make_db_rows <- function(jobs, make_empty = FALSE){
  if(make_empty){
    tibble::tibble(
      result_id = "",
      slurm_id = "",
      timestamp = lubridate::now(),
      jobs_blob = blob::new_blob(),
      .rows = 0
    )
  }else{
    tibble::tibble(
      result_id = vapply(jobs, \(j) j$result_id, FUN.VALUE = character(1L)),
      slurm_id = vapply(jobs, \(j) get_slurm_id(j), FUN.VALUE = character(1L)),
      timestamp = lubridate::now(),
      jobs_blob = lapply(jobs, \(j) qs::qserialize(j))
    )
  }
}

store_jobs <- function(jobs){
  RSQLite::dbAppendTable(.DB_CONNECTION(), "job_overview", value = make_db_rows(jobs))
}

#' Return the jobs stored in the storage table
#'
#' @param n the number of jobs to return. Default: `10`
#' @param raw flag indicating if the `tbl` of the
#'   database connection is returned or if the data is
#'   sorted by `timestamp` and `collect()` is called.
#'   Default: `FALSE`
#'
#'
#' @export
get_jobs <- function(n = 10, raw = FALSE){
  if(raw){
    dplyr::tbl(.DB_CONNECTION(), "job_overview")
  }else{
    dplyr::tbl(.DB_CONNECTION(), "job_overview") %>%
      dplyr::slice_min(timestamp, with_ties = FALSE, n = n) %>%
      dplyr::collect(n = n) %>%
      dplyr::mutate(job = lapply(job, qs::qdeserialize))
  }
}

#' Apply function to all dependencies of a job and return a list
#'
#' @param x a job or a list of jobs
#' @param FUN the function that is called with a job object
#'
#' @export
lapply_to_dependencies <- function(x, FUN){
  if("dependencies" %in% names(x)){
    c(list(FUN(x)), unlist(lapply(x$dependencies, function(dep) lapply_to_dependencies(dep, FUN)), recursive = FALSE))
  }else if(is.list(x)){
    lapply(x, function(.x) lapply_to_dependencies(.x, FUN))
  }else{
    stop("Cannot handle object of type: ", paste0(class(x), collapse = ", "))
  }
}


#' Cancel depending jobs
#'
#' @export
cancel_all_depending_jobs <- function(job){
  # Start jobs on which this job depends
  slurm_ids <- unlist(lapply_to_dependencies(job, get_slurm_id))

  submission <- glue::glue("scancel {paste0(slurm_ids, collapse = ' ')}") %>% glue::trim()

  message("Cancelling jobs")
  message(submission)
  system(submission, wait = TRUE)
  invisible(job)
}

#' Cancel one individual job
#'
#' @export
cancel_job <- function(job){
  if(job_status(job) %in% c("failed")){
    message("Job is not running. Status: ", job_status(job))
  }else{
    # Cancel all
    slurm_id <- get_slurm_id(job)

    submission <- glue::glue("scancel {slurm_id}") %>% glue::trim()

    message("Cancelling jobs")
    message(submission)
    system(submission, wait = TRUE)
  }
  invisible(job)
}









