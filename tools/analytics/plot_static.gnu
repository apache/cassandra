# Run with: gnuplot ~/Downloads/db-3558/plot.sh
# Documentation: http://www.gnuplot.info/docs_5.2/Gnuplot_5.2.pdf

path = "."
outputPath = path."/"

# define the output type and size
set terminal png size 2000,800 linewidth 2

# the first line makes it skip the first row and use it for the legend otherwise we must specify for every plot:
# ... every ::1 using 1:n with lines title "blah"
set key autotitle columnhead
set datafile separator ","

# timestamp ms,W,num compactions,live sstables,space used bytes,Num inserted,Num read,% inserted,% read,Read IO,Read IO stddev,Write IO,Write IO stddev,Tot IO,Tot IO stddev, Num pending, WA

#datasets = "R100_W0 R80_W20 R50_W50 R20_W80 R0_W100"
#datasets = "R80_W20 R50_W50 R20_W80"
datasets = "R50_W50"
N = words(datasets)

dataset(i) = word(datasets, i)
file(i) = sprintf("%s/testStaticAnalysis_%s-avg.csv", path, word(datasets, i))

set xlabel 'W'

set output outputPath.'live_sstables.png'
set title "Live sstables"
set ylabel 'Num sstables'
plot for [i=1:N] file(i) using 2:4 with linespoints title dataset(i)

set output outputPath.'space_used.png'
set title "Space used"
set ylabel 'Bytes Used'
plot for [i=1:N] file(i) using 2:5 with linespoints title dataset(i)


set ylabel 'ms'

set output outputPath.'read_io_cost.png'
set title "Read IO cost"
plot for [i=1:N] file(i) using 2:10:11 with yerrorbars lt i title "", \
     for [i=1:N] file(i) using 2:10 smooth acsplines lt i title dataset(i)

set output outputPath.'write_io_cost.png'
set title "Write IO cost"
plot for [i=1:N] file(i) using 2:12:13 with yerrorbars lt i title "", \
     for [i=1:N] file(i) using 2:12 smooth acsplines lt i title dataset(i)

set output outputPath.'tot_io_cost.png'
set title "Tot. IO cost"
plot for [i=1:N] file(i) using 2:14:15 with yerrorbars lt i title "", \
     for [i=1:N] file(i) using 2:14 smooth acsplines lt i title dataset(i)


set output outputPath.'wa.png'
set title "Write Amplification"
set ylabel 'WA'
plot for [i=1:N] file(i) using 2:17 with linespoints title dataset(i)