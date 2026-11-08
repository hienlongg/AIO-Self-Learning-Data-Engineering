READINGS := scd2 data-mart streaming-transformations stream-transformations kafka-delivery-semantics-and-dedup
.PHONY: all clean $(READINGS)
all: $(READINGS)
$(READINGS):
	cd readings/$@ && latexmk -pdf -outdir=../../build/$@ reading.tex
clean:
	rm -rf build/*
