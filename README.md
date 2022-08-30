# Media-Analysis

This repository includes code to replicate my dissertation, "The Impact of Media of Political Polarization: Evidence from the United States." The files are defined as follows:

1. Master_text_sql conducts the text mining of Fox News and CNN, then writes results into SQLite
2.Congress_pdf_scrape pulls text data from the records of Congress, then formats that data and tracks individual speakers. Results are written into SQlite
3. SQLite_pull functions load text data recorded in SQLite into python, then pivot that information into counts.  
4. The 'compute_new.R' script impliments the novel estimator proposed by Gentzkow, Shapiro, Taddy (2019). The central estimation routine returns the phi parameters used to recover our polarization indices.  It also returns most polarizing terms and media personalities. 
5. IV implimentas instrumental variables estimation on our final polarization measures to establish casuality between the two series.  
