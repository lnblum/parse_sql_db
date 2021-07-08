#This script is meant to extract bin and bin scaffold tables from sqlite database bin.sdb
$taxon_id=$ARGV[0]; #taxon_id input option flag
$file=</global/dna/projectdirs/microbial/img_web_data_merfs/$taxon_id/assembled/bin.sdb>; 
system("sqlite3 -header -separator ',' $file 'SELECT * from bin' >> /global/cscratch1/sd/lblum/bins/${taxon_id}_bin.txt"); #converts the file to a tsv table
system("sqlite3 -header -separator ',' $file 'SELECT * from bin_scaffolds' >> /global/cscratch1/sd/lblum/bins/${taxon_id}_bin_scaffolds.txt");