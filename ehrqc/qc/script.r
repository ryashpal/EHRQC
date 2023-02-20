args <- commandArgs(trailingOnly = TRUE)

## same function as above
detect_outliers <- function(path.to.input, path.to.output) {

    data.in <- read.csv(path.to.input)
    y1 <- DDoutlier::KNN_AGG(data.in)
    y2 <- DDoutlier::LOF(data.in)
    # y3 <- DDoutlier::COF(data.in, k=10)
    y4 <- DDoutlier::INFLO(data.in)
    y5 <- DDoutlier::KDEOS(data.in)
    y6 <- DDoutlier::LDF(data.in)
    # # y7 <- DDoutlier::LDOF(data.in, k=10)
    # Y <- cbind.data.frame(y1, y2, y3, y4, y5, y6$LDF, y7)
    Y <- cbind.data.frame(y1, y2, y4, y5, y6$LDF)
    is.na(Y) <- sapply(Y, is.infinite)
    Y[is.na(Y)] <- 0
    ens1 <- outlierensembles::irt_ensemble(Y)
    data.out <- cbind.data.frame(data.in, y1, y2, y4, y5, y6$LDF, ens1$scores)
    colnames(data.out) <- c(colnames(data.in), 'y_knn_agg', 'y_lof', 'y_inflo', 'y_kdeos', 'y_ldf', 'ensemble_scores')
    write.csv(data.out, path.to.output, row.names = FALSE)

}

detect_outliers(path.to.input = args[1], path.to.output = args[2])
