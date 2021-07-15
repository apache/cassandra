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
#datasets = "R50_W50 R20_W80"
datasets = "R50_W50"
N = words(datasets)

dataset(i) = word(datasets, i)
file(i) = sprintf("%s/testAdaptiveController_%s.csv", path, word(datasets, i))

set xlabel 'Time (ms)'
#set xrange [0:300000]

set output outputPath.'live_sstables.png'
set title "Live sstables"
set ylabel 'Num sstables'
plot for [i=1:N] file(i) using 1:4 with linespoints title dataset(i)

set output outputPath.'space_used.png'
set title "Space used"
set ylabel 'Bytes Used'
plot for [i=1:N] file(i) using 1:5 with linespoints title dataset(i)


set ylabel 'IO cost (ms)'
set y2label 'W'
set y2range [-20:100]
set y2tics -12, 4
set ytics nomirror

do for [i=1:N] {

	set output outputPath.'read_io_cost_'.dataset(i).'.png'
	set title "Read IO cost"
	plot file(i) using 1:10:11 with yerrorbars lt 1 title "", \
    	 file(i) using 1:10 smooth acsplines lt 1 title dataset(i), \
     	 file(i) using 1:2 with lines lt 2 axis x1y2 title "W"

	set output outputPath.'write_io_cost_'.dataset(i).'.png'
	set title "Write IO cost"
	plot file(i) using 1:12:13 with yerrorbars lt 1 title "", \
	     file(i) using 1:12 smooth acsplines lt 1 title dataset(i), \
	     file(i) using 1:2 with lines lt 2 axis x1y2 title "W"

	set output outputPath.'tot_io_cost_'.dataset(i).'.png'
	set title "Tot. IO cost"
	plot file(i) using 1:14:15 with yerrorbars lt 1 title "", \
	     file(i) using 1:14 smooth acsplines lt 1 title dataset(i), \
	     file(i) using 1:2 with lines lt 2 axis x1y2 title "W"

	set output outputPath.'wa'.dataset(i).'.png'
        set title "Write Amplification"
        set ylabel 'WA'
        plot file(i) using 1:17 with linespoints title 'WA', \
             file(i) using 1:2 with lines lt 3 axis x1y2 title "W"
}