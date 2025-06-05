# KBase Narrative Import Process

[KBase](https://kbase.us) offers useful tools for analyzing genomics data within [Narratives](https://docs.kbase.us/getting-started/quick-start).
By adding some metadata to your DTS transfer request, you can easily import your data into a KBase
narrative.

Here we describe a schema for the `instructions` field of a [DTS transfer POST request](https://dts.kbase.us/docs#/operations/post-api-v1-transfers).
This field is embedded in the `manifest.json` file written to the root of the destination folder for
a payload, and holds a JSON object that tells KBase's staging service how to import the contents of
the payload into a KBase narrative. This schema was originally proposed [in a GitHub issue](https://github.com/kbase/dts/issues/79).

What we describe here is relevant only to KBase Narrative imports--the structure of the `instructions`
field is specific to the destination of a transfer to allow the DTS to interact reasonably with
specific systems and organizations.

## JSON Object Structure

The `instructions` field included in the transfer POST request has the following fields:

* `protocol`: contains the string `"KBase narrative import"`
* `objects`: contains an object interpreted as a dictionary whose **keys are supported KBase data
  types** and whose **values are lists of JSON objects**, each with fields specific to that data
  type. Each data types is described in the following section.

Here's a brief example of such a JSON object that annotates two Genbank genome files:

```
{
    "instructions": {
        "protocol": "KBase narrative import",
        "objects": {
            "genbank_genome": [
                {
                    "staging_file_subdir_path": "path/to/some_genome.gbk",
                    "genome_name": "some_genome"
                    "genome_type": null,
                    "source": null,
                    "release": null,
                    "genetic_code": null,
                    "scientific_name": null,
                    "generate_ids_if_needed": null,
                    "generate_missing_genes": null
                }, 
                {
                    "staging_file_subdir_path": "path/to/some_other_genome.gbk",
                    "genome_name": "some_other_genome"
                    "genome_type": null,
                    "source": null,
                    "release": null,
                    "genetic_code": null,
                    "scientific_name": null,
                    "generate_ids_if_needed": null,
                    "generate_missing_genes": null
                }
            ]
        }
    }
}
```

## Supported KBase Data Types

The data types supported by the KBase staging service are listed below with their fields. These data
types are based on the [KBase staging service's import specification templates](https://github.com/kbase/staging_service/tree/master/import_specifications/templates).

!!! note "All fields required!"

    Every object of a given data type must have all of its fields specified, with `null` values
    indicating empty fields. While inconvenient, this complete specification is required by the
    current implementation of the KBase staging service.

Fields that must be non-`null` are marked **bold** below.

#### Assembly (`assembly`)

* `**staging_file_subdir_path**`: a string containing the path to the file, rooted in the directory
  containing the manifest
* `**assembly_name**`: the name of the assembly object
* `type`: one of the following strings:
    * `"draft isolate"`
    * `"finished isolate"`
    * `"mag"`
    * `"sag"`
    * `"virus"`
    * `"plasmid"`
    * `"construct"`
    * `"metagenome"`
* `min_contig_length`: an integer containing the minimum length of a contig within the assembly

### Genbank genome (`genback_genome`)

* `**staging_file_subdir_path**`: a string containing the path to the file, rooted in the directory
  containing the manifest
* `**genome_name**`: the name of the genome object
* `genome_type`: one of the following strings:
    * `"draft isolate"`
    * `"finished isolate"`
    * `"mag"`
    * `"sag"`
    * `"virus"`
    * `"plasmid"`
    * `"construct"`
* `source`: one of the following strings:
    * `"RefSeq user"`
    * `"Ensembl user"`
    * `"Other"`
* `release`: a string
* `genetic_code`: an integer
* `scientific_name`: the scientific name of the genome
* `generate_ids_if_needed`: a string
* `generate_missing_genes`: a string

### GFF+FASTA genome (`gff_genome`)

* `**fasta_file**`: a string containing the path to a FASTA file, rooted in the directory
  containing the manifest
* `**gff_file**`: a string containing the path to a GFF file, rooted in the directory
  containing the manifest
* `**genome_name**`: the name of the genome object
* `genome_type`: one of the following strings:
    * `"draft isolate"`
    * `"finished isolate"`
    * `"fungi"`
    * `"mag"`
    * `"other Eukaryote"`
    * `"plant"`
    * `"sag"`
    * `"virus"`
    * `"plasmid"`
    * `"construct"`
* `scientific_name`: the scientic name of the genome
* `source`: one of the following strings:
    * `"RefSeq user"`
    * `"Ensembl user"`
    * `"JGI"`
    * `"Other"`
* `taxon_wsname`: a string
* `release`: a string
* `genetic_code`: an integer
* `generate_missing_genes`: a string

### GFF+FASTA metagenome (`gff_metagenome`)

* `**fasta_file**`: a string containing the path to a FASTA file, rooted in the directory
  containing the manifest
* `**gff_file**`: a string containing the path to a GFF file, rooted in the directory
  containing the manifest
* `**genome_name**`: the name of the genome object
* `source`: "str, ['EBI user', 'IMG user', 'JGI user', 'BGI user', 'Other']",
* `source`: one of the following strings:
    * `"EBI user"`
    * `"IMG user"`
    * `"JGI user"`
    * `"BGI user"`
    * `"Other"`
* `release`: a string
* `genetic_code`: an integer
* `generate_missing_genes`: a string

### Interleaved FASTQ reads (`fastq_reads_interleaved`)

* `**fastq_fwd_staging_file_name**`: a string containing the path to the file, rooted in the directory
  containing the manifest
* `**name**`: the name of the interleaved FASTQ reads object
* `sequencing_tech`: one of the following strings:
    * `"Illumina"`
    * `"PacBio CLR"`
    * `"PacBio CCS"`
    * `"IonTorrent"`
    * `"NanoPore"`
    * `"Unknown"`
* `single_genome`: a string
* `read_orientation_outward`: a string
* `insert_size_std_dev`: a float indicating the standard deviation of the insert size distribution
* `insert_size_mean`: a float indicating the mean of the insert size distribution

### Noninterleaved FASTQ reads (`fastq_reads_noninterleaved`)

* `**fastq_fwd_staging_file_name**`: a string containing the path to the forward reads file, rooted
  in the directory containing the manifest
* `**fastq_rev_staging_file_name**`: a string containing the path to the reverse reads file, rooted
  in the directory containing the manifest
* `**name**`: the name of the noninterleaved FASTQ reads object
* `sequencing_tech`: one of the following strings:
    * `"Illumina"`
    * `"PacBio CLR"`
    * `"PacBio CCS"`
    * `"IonTorrent"`
    * `"NanoPore"`
    * `"Unknown"`
* `single_genome`: a string
* `read_orientation_outward`: a string
* `insert_size_std_dev`: a float indicating the standard deviation of the insert size distribution
* `insert_size_mean`: a float indicating the mean of the insert size distribution

### SRA reads (`sra_reads`)

* `**sra_staging_file_name**`: a string containing the path to the SRA staging file, rooted in the
  directory containing the manifest
* `**name**`: the name of the SRA reads object
* `sequencing_tech`: one of the following strings:
    * `"Illumina"`
    * `"PacBio CLR"`
    * `"PacBio CCS"`
    * `"IonTorrent"`
    * `"NanoPore"`
    * `"Unknown"`
* `single_genome`: a string
* `read_orientation_outward`: a string
* `insert_size_std_dev`: a float indicating the standard deviation of the insert size distribution
* `insert_size_mean`: a float indicating the mean of the insert size distribution
