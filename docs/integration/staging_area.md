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
files between different databases and organizations. Globus is [widely used in
the scientific research community](https://www.globus.org/user-stories) by
universities and DOE national laboratories, and its design reflects its focus on
helping researchers share and access data. Additionally, Globus can be
integrated with other file storage and transfer platforms like
[Amazon S3](https://www.globus.org/connectors/amazon-s3),
[Google](https://www.globus.org/connectors/google-cloud), and many others.
This interoperability allows Globus clients to interoperate seamlessly even in
heterogeneous technological settings.

## Setting up a Staging Area for Globus

If your organization has a [Globus subscription](https://www.globus.org/why-subscribe),
you'll have an easy time configuring a file staging area that can be integrated
with the DTS. Globus has [extensive documentation](https://www.globus.org/data-sharing)
to help you understand how to share your data effectively.

To make your files available to the DTS, you'll need to set up a Globus guest
collection. The following links can help you configure your Globus setup:

* [How to Share Data Using Globus](https://docs.globus.org/guides/tutorials/manage-files/share-files/):
  a step-by-step guide to creating a guest collection that can serve as a
  staging area for your organization
* [Storage Connector Usage Guides](https://docs.globus.org/guides/tutorials/storage-connectors/):
  a landing page for documentation about Globus's [connectors](https://www.globus.org/connectors),
  which allow you to access data stored on other platforms like Amazon S3 and
  Google Cloud Storage
* [The Modern Research Data Portal](https://docs.globus.org/guides/recipes/modern-research-data-portal/):
  an article describing a design pattern for providing secure, scalable, and
  high performance access to research data
* [Globus Connect Server](https://docs.globus.org/globus-connect-server/):
  information about how Globus can be deployed within your organization
  (probably most useful to an IT department!)

In particular, the first link walks you through the steps of creating a guest
collection. When you've set up a guest collection in which you can stage files
for transmission and receipt, the DTS team can work with you to authorize the
Data Transfer Service to use this guest collection so it can access these files.
