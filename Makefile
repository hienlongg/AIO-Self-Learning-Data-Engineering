READINGS := scd2 data-mart
.PHONY: all clean $(READINGS)
all: $(READINGS)
$(READINGS):
	cd readings/$@ && latexmk -pdf -outdir=../../build/$@ reading.tex
clean:
	rm -rf build/*
