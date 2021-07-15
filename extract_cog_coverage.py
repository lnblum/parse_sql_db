import os
import sqlite3
import sys
tax_oid = sys.argv[1]


def cog2gene(tax_oid):
    """
    Locate COGs of interest in gene2cog sqlite3 database, extract associated gene_ids. Append to one text file for each gene_cog database.
    Inputs:tax_oid: list of taxon_ids for metagenomes to perform task on
    """
    cog_sdb_list = []
    cog_dir = '/global/dna/projectdirs/microbial/img_web_data_merfs/' + str(tax_oid) + '/assembled/gene2cog/'
    sdb_query = 'SELECT gene_oid, cog FROM gene_cog where cog in ("COG0012", "COG0016", "COG0018", "COG0172", "COG0215", "COG0495", "COG0525", "COG0533", "COG0541", "COG0552");'
    output = os.path.join('/global/cscratch1/sd/lblum/cog_cov', str(tax_oid))
    gene2cog = os.path.join(output, 'gene2cog.txt')
    
    for file in os.listdir(cog_dir):
        if file.endswith(".sdb"):
            cog_sdb_list.append(os.path.join(cog_dir, file))

    for sdb in cog_sdb_list:
        conn = sqlite3.connect(sdb)
        c = conn.cursor()
        c.execute(sdb_query) 
        with open(gene2cog, 'a') as genes:
            for gene, cog in c.fetchall():
                genes.write(gene + "\t"+cog +"\n" )
        conn.close()

def gene2scaffold(tax_oid):
    """
    Locate scaffold_ids associated with gene_ids. Append to one text file for each scaffold_genes database.
    """
    gene_sdb_list = []
    gene_dir = '/global/dna/projectdirs/microbial/img_web_data_merfs/' + str(tax_oid) + '/assembled/scaffold_genes/'
    output = os.path.join('/global/cscratch1/sd/lblum/cog_cov', str(tax_oid))
    gene2cog = os.path.join(output, 'gene2cog.txt')
    gene2scaffold = os.path.join(output, 'gene2scaffold.txt')
            
    for file in os.listdir(gene_dir):
        if file.endswith(".sdb"):
            gene_sdb_list.append(os.path.join(gene_dir, file))
    
    for sdb in gene_sdb_list:
        conn = sqlite3.connect(sdb)
        c = conn.cursor()
        c.execute('''CREATE TEMP TABLE IF NOT EXISTS mytable(gene TEXT, cog TEXT)''')
        sql_insert = ''' INSERT INTO mytable(gene, cog) VALUES (?, ?) '''
        with open(gene2cog, 'r') as fr:
            for line in fr.readlines():
            # parse the txt file
                line = line.replace('\n', '').split('\t')
                t, f = line
                c.execute(sql_insert, (t, f))
        
        c.execute('SELECT gene, cog, scaffold_oid FROM mytable INNER JOIN scaffold_genes on scaffold_genes.gene_oid = mytable.gene')       
        with open(gene2scaffold, 'a') as gene2scaff:
            for gene, cog, scaffold in c.fetchall():
                gene2scaff.write(gene + "\t" + cog + "\t" + scaffold +"\n")                      
        conn.close()

def scaffold_coverage(tax_oid):
    """
    Connect scaffold ids to coverage data. Write to a text file. 
    """
    scaff_path = '/global/dna/projectdirs/microbial/img_web_data_merfs/' + str(tax_oid) + '/assembled/scaffold_depth.sdb'
    output = os.path.join('/global/cscratch1/sd/lblum/cog_cov', str(tax_oid))
    scaff_cov = os.path.join(output, 'scaff_cov.txt')
    gene2scaffold = os.path.join(output, 'gene2scaffold.txt')
    
    conn = sqlite3.connect(scaff_path)
    c = conn.cursor()
    
    c.execute('''CREATE TEMP TABLE IF NOT EXISTS mytable(gene TEXT, cog TEXT, scaff INTEGER)''')
    sql_insert = ''' INSERT INTO mytable(gene, cog, scaff) VALUES (?, ?, ?) '''
    with open(gene2scaffold, 'r') as fr:
        for line in fr.readlines():
        # parse the txt file
            line = line.replace('\n', '').split('\t')
            t, f, s = line
            c.execute(sql_insert, (t, f, s))
                
    c.execute('SELECT gene, cog, scaff, depth FROM mytable INNER JOIN scaffold_depth on scaffold_depth.scaffold_oid = mytable.scaff')       
        
    with open(scaff_cov, 'a') as sc:
        for gene, cog, scaffold, depth  in c.fetchall():
            sc.write(gene + "\t" + cog + "\t" + scaffold + "\t" + str(depth) + "\n")                      
    conn.close()
    
cog2gene(tax_oid)
gene2scaffold(tax_oid)
scaffold_coverage(tax_oid)