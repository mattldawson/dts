# Granting the DTS Access to a Globus Endpoint

The Data Transfer Service (DTS) relies heavily on [Globus](https://www.globus.org/)
for performing file transfers between different databases. Globus is an elaborate
and continuously evolving platform, so configuring access from an application
can be confusing. Here we describe all the things you need to know to grant
the DTS access to a Globus endpoint.

## Globus Glossary

Globus has its own set of terminology that is slightly different from that we've
 used to describe the DTS, so let's clarify some definitions first.

* **Globus Endpoint**: A Globus endpoint is a server running Globus software,
  providing access to a filesystem that can be shared with Globus users. To the
  DTS, an endpoint is "a thing that can send and receive files."
* **Globus Collection**: A Globus Collection is a portion of a filesystem on a
  Globus Endpoint associated with roles and permissions for Globus users. It is
  not a server--it's a set of metadata that tells Globus which users have what
  access privileges on a Globus Endpoint.
* **Globus Guest Collection**: A Guest Collection is a Globus Collection that allows
  a Globus user to share files on with other Globus users and with applications.
  In particular, a Guest Collections is the _only mechanism_ that can provide
  client applications with access to resources on Globus endpoints. This is the
  closest concept to what the DTS considers an endpoint.

## Setting up Access to a Globus Endpoint

[This guide](https://docs.globus.org/guides/recipes/automate-with-service-account/)
gives a complete set of instructions using the terminology above. Below, we briefly
summarize the steps in the guide. Of course, you need a Globus user account to play
this game.

1. **Obtain an Application/Service Credential for the DTS.** The credential
   consists of a unique client ID and an associated client secret. The client ID
   can be used to identify the DTS as an entity that can be granted access
   permissions. Of course, the primary instance of the DTS already has one of
   these.

2. **Create a Guest Collection on the Globus Endpoint or on an existing Collection.**
   Without a Guest Collection, you can't grant the DTS access to anything. You might
   have to poke around a bit to find an endpoint or existing collection that (a) you
   have access to and (b) that exposes the resources that you want to grant to the
   DTS.

3. **Grant the DTS read or read/write access to the Guest Collection.** Since
   the DTS has its own client ID, you can grant it access to a Guest Collection
   just as you would any other Globus user.

The DTS stores its Globus credentials (client ID, client secret) in environment
variables to prevent them from being read from a file or mined from the executable.
The [deployment](deployment.md) section describes how these environment variables
are managed in practice.
