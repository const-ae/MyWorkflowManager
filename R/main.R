
#'
#' @importFrom magrittr `%>%`
NULL

.OUTPUT_FOLDER <- function(){
  out <- Sys.getenv("MYWORKFLOWMANAGER_OUTPUT_FOLDER")
  if(is.null(out) || out == ""){
    stop("Please call 'init()' and set the output folder")
  }
  out
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

  Sys.setenv("MYWORKFLOWMANAGER_OUTPUT_FOLDER" = output_folder)
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
  }else{
    message("Job is already running: ", job_status(job))
  }
  invisible(job)
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


## I don't think I ever used this function
# is_job_defective <- function(job){
#   stopifnot(is.job(job))
#   job_okay <- file.exists(file.path(.OUTPUT_FOLDER(), "params", job$params_id)) && file.exists(file.path(.OUTPUT_FOLDER(), "scripts", job$script_id))
#   if(job_okay){
#     deps_defect <- unlist(lapply_to_dependencies(job$dependencies, is_job_defective))
#     if(is.null(deps_defect)){
#       deps_defect <- FALSE
#     }
#     any(deps_defect)
#   }else{
#     TRUE
#   }
# }

## In practice, I never used this set of functions to delete jobs
# find_dependent_objects_for_result <- function(result_ids){
#   if(length(result_ids) == 0){
#     list(params = character(0L), results = character(0L), dependencies = list())
#   }else{
#     dependent_params <- character(0L)
#     # Iterate in batches to speed up grep, but also don't make a too big regex
#     for(idx in seq(1, length(result_ids), by = 1000)){
#       search_command <- paste0("grep -r ", file.path(.OUTPUT_FOLDER(), "params"), " -l -e '", paste0(result_ids[seq(idx, min(length(result_ids), idx+999))], collapse = "\\|"), "'")
#       dependent_params <- c(suppressWarnings(system(search_command, intern = TRUE)))
#     }
#     dependent_params <- str_remove(dependent_params, file.path(.OUTPUT_FOLDER(), "params"))
#     direct_res <- unlist(lapply(dependent_params, function(params_id) list.files(file.path(.OUTPUT_FOLDER(), "results"), pattern = params_id)))
#     if(is.null(direct_res)){
#       direct_res <- character(0L)
#     }
#     list(params = dependent_params, results = direct_res, dependencies = list(find_dependent_objects_for_result(direct_res)))
#   }
# }
#
# find_dependent_objects_for_script <- function(script_id){
#   # Find all results that directly come from this script
#   direct_res <- list.files(file.path(.OUTPUT_FOLDER(), "results"), pattern = script_id)
#   dependent_objects <- find_dependent_objects_for_result(direct_res)
#   all_params <- unlist(lapply_to_dependencies(dependent_objects, function(e) e$params))
#   all_results <- c(direct_res, unlist(lapply_to_dependencies(dependent_objects, function(e) e$results)))
#   list(params = all_params, results = all_results)
# }
#
#
# careful_delete_file <- function(path){
#   for(p in path){
#     if(file.exists(p)){
#       file.remove(p)
#     }
#   }
# }
#
# remove_dependent_objects_for_script <- function(script_id, check = TRUE){
#   res <- find_dependent_objects_for_script(script_id)
#
#   logs <- purrr::simplify(lapply(res$results, function(res) list.files(file.path(.OUTPUT_FOLDER(), "logs"), pattern = paste0("id_", res, "-slurmid_"))))
#   .remove_objects(all_params = res$params,
#                   all_results = res$results,
#                   all_slurm_job_files = res$results,
#                   all_durations = res$results,
#                   all_logs = logs,
#                   script = script_id,
#                   check = check)
#
#
# }
#
# remove_job_files <- function(jobs, check = TRUE){
#   if(is.job(jobs)){
#     jobs <- list(jobs)
#   }
#
#   params <- purrr::simplify(lapply(jobs, function(job){
#     purrr::simplify(lapply_to_dependencies(job, function(d) d$params_id), .type = "character")
#   }), .type = "character")
#   results <- purrr::simplify(lapply(jobs, function(job){
#     purrr::simplify(lapply_to_dependencies(job, function(d) d$result_id), .type = "character")
#   }), .type = "character")
#
#   dependent_objects <- find_dependent_objects_for_result(results)
#   all_params <- c(params, unlist(lapply_to_dependencies(dependent_objects, function(e) e$params)))
#   all_results <- c(results, unlist(lapply_to_dependencies(dependent_objects, function(e) e$results)))
#   all_slurm_job_files <- all_results
#   all_logs <- purrr::simplify(lapply(all_results, function(res) list.files(file.path(.OUTPUT_FOLDER(), "logs"), pattern = paste0("id_", res, "-slurmid_"))))
#
#   .remove_objects(all_params = all_params,
#                   all_results = all_results,
#                   all_slurm_job_files = all_results,
#                   all_durations = all_results,
#                   all_logs = all_logs,
#                   check = check)
# }
#
#
# .remove_objects <- function(all_params = character(0L),
#                             all_results = character(0L),
#                             all_slurm_job_files = character(0L),
#                             all_durations = character(0L),
#                             all_logs = character(0L),
#                             script = character(0L),
#                             check = TRUE){
#
#   all_params <- unique(all_params)
#   all_results <- unique(all_results)
#   all_slurm_job_files <- unique(all_slurm_job_files)
#   all_durations <- unique(all_durations)
#   all_logs <- unique(all_logs)
#   script <- unique(script)
#
#
#   all_params <- all_params[file.exists(file.path(.OUTPUT_FOLDER(), "params", all_params))]
#   all_results <- all_results[file.exists(file.path(.OUTPUT_FOLDER(), "results", all_results))]
#   all_slurm_job_files <- all_slurm_job_files[file.exists(file.path(.OUTPUT_FOLDER(), "slurm_job_overview", all_slurm_job_files))]
#   all_durations <- all_durations[file.exists(file.path(.OUTPUT_FOLDER(), "duration/", all_durations))]
#   all_logs <- all_logs[file.exists(file.path(.OUTPUT_FOLDER(), "logs", all_logs))]
#   script <- script[file.exists(file.path(.OUTPUT_FOLDER(), "scripts/", script))]
#
#   if(check){
#     if(length(all_results) == 0 && length(all_params) == 0 && length(all_slurm_job_files) == 0 && length(all_durations) == 0 && length(all_logs) == 0 && length(script) == 0){
#       message("Nothing to be deleted")
#       return(invisible(NULL))
#     }
#     message("Careful !!!!!")
#     message("This operation will delete ", length(all_params), " parameter objects, ", length(all_logs), " logs, ", length(all_slurm_job_files), " slurm_job_files, ", length(all_durations), " duration files, ", length(script), " script and ", length(all_results), " result objects")
#     resp <- readline("Are you sure you want to continue? ([y] for yes /[n] for no/ [c] for cancel / [m] for more details) > ")
#     if(resp == "m"){
#       message("All parameters: ", paste0(all_params, ", "))
#       message("All results: ", paste0(all_results, ", "))
#       message("All logs: ", paste0(all_logs, ", "))
#       message("All slurm_job_files: ", paste0(all_slurm_job_files, ", "))
#       message("All duration files: ", paste0(all_durations, ", "))
#       message("The script: ", paste0(script, ", "))
#     }
#     counter <- 1
#     while(! resp %in% c("y", "n", "c") && counter <= 5){
#       resp <- readline("Are you sure you want to continue? (y/n/c) > ")
#       counter <- counter + 1
#     }
#     if(counter > 5){
#       stop("No valid input received")
#     }else if(resp == "y"){
#       # Delete files
#       careful_delete_file(file.path(.OUTPUT_FOLDER(), "params/", all_params))
#       careful_delete_file(file.path(.OUTPUT_FOLDER(), "results/", all_results))
#       careful_delete_file(file.path(.OUTPUT_FOLDER(), "slurm_job_overview/", all_slurm_job_files))
#       careful_delete_file(file.path(.OUTPUT_FOLDER(), "duration/", all_durations))
#       careful_delete_file(file.path(.OUTPUT_FOLDER(), "logs/", all_logs))
#       careful_delete_file(file.path(.OUTPUT_FOLDER(), "scripts/", script))
#       message("Successfully deleted all files.")
#     }else{
#       message("Abort.")
#     }
#   }else{
#     careful_delete_file(file.path(.OUTPUT_FOLDER(), "params/", all_params))
#     careful_delete_file(file.path(.OUTPUT_FOLDER(), "results/", all_results))
#     careful_delete_file(file.path(.OUTPUT_FOLDER(), "slurm_job_overview/", all_slurm_job_files))
#     careful_delete_file(file.path(.OUTPUT_FOLDER(), "duration/", all_durations))
#     careful_delete_file(file.path(.OUTPUT_FOLDER(), "logs/", all_logs))
#     careful_delete_file(file.path(.OUTPUT_FOLDER(), "scripts/", script))
#   }
# }

