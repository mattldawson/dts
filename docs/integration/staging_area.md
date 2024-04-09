# Provide a Staging Area for Your Files

In previous sections we discussed how you can provide researchers with
information about the files in your database, both in the small with
file-specific metadata and in the large with a text-based search capability. Now
we turn our attention to the process of allowing these users to access these
files and transfer them to other locations for analysis and manipulation by
site-specific processes.

Recall that the Data Transfer System (DTS) orchestrates transfers of data
between the databases of participating organizations like yours. Transferring
such data invariably involves moving files from a "source" staging area in one
organization to a "destination" staging area in another. Here, a _staging area_
is a file system visible to some file transport mechanism such as
[Globus](https://www.globus.org/). To allow the DTS to access your data for
transfer, your organization must establish and maintain such a file staging
area.

At this time, the DTS relies on [Globus](https://www.globus.org) to move
files between different databases and organizations. Globus is widely used in
the scientific research community by universities and national laboratories, and
its design reflects its focus on helping researchers share and access data.
Additionally, Globus can be integrated with other file storage and transfer
services like [Amazon S3](https://www.globus.org/connectors/amazon-s3)
and [iRODS](https://www.globus.org/connectors/irods), which allows Globus
clients to interoperate seamlessly even in heterogeneous technological
settings.

## Setting up a Staging Area for Globus


## Example Configuration

* Example configuration: JGI and KBase Globus Guest Collections
