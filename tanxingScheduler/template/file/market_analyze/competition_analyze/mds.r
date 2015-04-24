#!/usr/bin/Rscript

args <- commandArgs(T);
input_file <- args[1];
dist.au <- read.table( input_file );  
fit <- cmdscale(dist.au, eig = TRUE, k = 2);
fit$points;








