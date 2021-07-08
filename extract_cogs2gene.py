def cog2gene(tax_oid):
    """
    Locate COGs of interest in gene2cog sqlite3 database, extract associated gene_ids.
    Inputs:tax_oid: list of taxon_ids for metagenomes to perform task on
    """
    cog_sdb_list = []
    cog_dir = '/global/dna/projectdirs/microbial/img_web_data_merfs/' + str(tax_oid) + '/assembled/gene2cog/'
    sdb_cog_query = 'SELECT gene_oid, cog FROM gene_cog where cog in ("COG0012", "COG0016", "COG0018", "COG0172", "COG0215", "COG0495", "COG0525", "COG0533", "COG0541", "COG0552");'
    output = os.path.join('/global/cscratch1/sd/lblum/cog_cov', str(tax_oid))
    gene2cog = os.path.join(output, 'gene2cog.txt')
    
    for file in os.listdir(cog_dir):
        if file.endswith(".sdb"):
            cog_sdb_list.append(os.path.join(cog_dir, file))

    for sdb in cog_sdb_list:
        conn = sqlite3.connect(sdb)
        c = conn.cursor()
        c.execute(sdb_cog_query) 
        with open(gene2cog, 'a') as genes:
            for gene, cog in c.fetchall():
                genes.write(gene + "\t"+cog +"\n" )
        conn.close()