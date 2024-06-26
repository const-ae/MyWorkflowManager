% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/main.R
\name{wrap_script}
\alias{wrap_script}
\title{Wrap a computation described by a script and some input parameters}
\usage{
wrap_script(
  script,
  params = list(),
  dependencies = list(),
  executor = c("R", "python"),
  extra_args = "",
  extra_slurm_args = "",
  duration = "01:00",
  memory = "1GB",
  n_cpus = 1
)
}
\arguments{
\item{script}{the file path to the script}

\item{params}{a list of simple parameters that can be converted to command
line options}

\item{dependencies}{a list of jobs whose results this job depends on}

\item{executor}{is it an R or a python script}

\item{extra_args}{a hack to specify the name of the conda environment that python loads}

\item{duration, memory, n_cpus}{the job resources}
}
\description{
Wrap a computation described by a script and some input parameters
}
