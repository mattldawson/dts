# KBase Narrative Import Process

[KBase](https://kbase.us) offers useful tools for analyzing genomics data within [Narratives](https://docs.kbase.us/getting-started/quick-start).
By adding some metadata to your DTS transfer request, you can easily import your data into a KBase
narrative.

This page gathers information from the [original proposal for narrative imports](https://github.com/kbase/dts/issues/79).

data_type: assembly

{
    "*staging_file_subdir_path": "str, file path",
    "*assembly_name": "str, object_name",
    "type": "str, ['draft isolate', 'finished isolate', 'mag', 'sag', 'virus', 'plasmid', 'construct', 'metagenome']",
    "min_contig_length": "int"
}

Genbank genome
data_type: genbank_genome

{
    "*staging_file_subdir_path": "str, file path",
    "*genome_name": "str, object_name",
    "genome_type": "str, ['draft isolate', 'finished isolate', 'mag', 'sag', 'virus', 'plasmid', 'construct']",
    "source": "str, ['RefSeq user', 'Ensembl user', 'Other']",
    "release": "str",
    "genetic_code": "int",
    "scientific_name": "str",
    "generate_ids_if_needed": "str",
    "generate_missing_genes": "str"
}

GFF+FASTA genome
data_type: gff_genome

{
    "*fasta_file": "str, file path",
    "*gff_file": "str, file path",
    "*genome_name": "str, object_name",
    "genome_type": "str, ['draft isolate', 'finished isolate', 'fungi', 'mag', 'other Eukaryote', 'plant', 'sag', 'virus', 'plasmid', 'construct']",
    "scientific_name": "str",
    "source": "str, ['RefSeq user', 'Ensembl user', 'JGI', 'Other']",
    "taxon_wsname": "str",
    "release": "str",
    "genetic_code": "int",
    "generate_missing_genes": "str"
}

GFF+FASTA metagenome
data_type: gff_metagenome

{
    "*fasta_file": "str, file path",
    "*gff_file": "str, file path",
    "*genome_name": "str, object_name",
    "source": "str, ['EBI user', 'IMG user', 'JGI user', 'BGI user', 'Other']",
    "release": "str",
    "genetic_code": "int",
    "generate_missing_genes": "str"
}

Interleaved FASTQ reads
data_type: fastq_reads_interleaved

{
    "*fastq_fwd_staging_file_name": "str, file path",
    "*name": "str, object_name",
    "sequencing_tech": "str, ['Illumina', 'PacBio CLR', 'PacBio CCS', 'IonTorrent', 'NanoPore', 'Unknown']",
    "single_genome": "str",
    "read_orientation_outward": "str",
    "insert_size_std_dev": "float",
    "insert_size_mean": "float"

}

Noninterleaved FASTQ reads
data_type: fastq_reads_noninterleaved

{
    "*fastq_fwd_staging_file_name": "str, file path",
    "*fastq_rev_staging_file_name": "str, file path",
    "*name": "str, object_name",
    "sequencing_tech": "str, ['Illumina', 'PacBio CLR', 'PacBio CCS', 'IonTorrent', 'NanoPore', 'Unknown']",
    "single_genome": "str",
    "read_orientation_outward": "str",
    "insert_size_std_dev": "float",
    "insert_size_mean": "float"
}

SRA reads
data_type: sra_reads

{
    "*sra_staging_file_name": "str, file path",
    "*name": "str, object_name",
    "sequencing_tech": "str, ['Illumina', 'PacBio CLR', 'PacBio CCS', 'IonTorrent', 'NanoPore', 'Unknown']",
    "single_genome": "str",
    "read_orientation_outward": "str",
    "insert_size_std_dev": "float",
    "insert_size_mean": "float"
}

A simple example of just the required values for, say, a genbank genome would be:

"instructions": {
    "data_type": "genbank_genome",
    "parameters": {
        "staging_file_subdir_path": "path/to/some_genome.gbk",
        "genome_name": "some_genome",
        "genome_type": null,
        "source": null,
        "release": null,
        "genetic_code": null,
        "scientific_name": null,
        "generate_ids_if_needed": null,
        "generate_missing_genes": null
    }
}


Just noting here that all fields for each import data type must be present, even if the values are
null. A bit annoying, I understand, but this comes from some deeper requirements in the staging
service and narrative that are currently out-of-scope to fix. I'll edit the light-schema comment
above as well.
