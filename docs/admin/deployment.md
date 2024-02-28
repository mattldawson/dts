# Deploying DTS via Docker

You can use the `Dockerfile` and `dts.yaml` files in the `deployment` folder to
build a Docker image for DTS. The Docker image contains two files:

1. `/bin/dts`: the statically-linked `dts` executable
2. `/etc/dts.yaml`: a [DTS configuration file](config.md) with embedded
   environment variables that control parameters of interest

This image can be deployed in any Docker-friendly environment. The use of
environment variables in the configuration file allows you to configure
DTS without regenerating the image.

## Deploying to NERSC's Spin Environment

DTS is hosted in NERSC's [Spin](https://www.nersc.gov/systems/spin/)
environment under [Rancher 2](https://rancher2.spin.nersc.gov/login).
It runs in the `Production` environment under the `kbase` organization.
You can read about Spin in NERSC's documentation, and Rancher 2
[here](https://rancher.com/docs/rancher/v2.x/en/). The documentation
isn't great, but fortunately there's not a lot to know--most of the
materials you'll need are right here in this directory.

Deploying DTS to Spin involves

1. updating and pushing a new Docker image with any code changes and
   documentation updates
2. editing the `dts` Spin deployment via NERSC's
   [Rancher 2](https://rancher2.spin.nersc.gov/login) console

Each of these steps are fairly simple.

Before you perform an update, take some time to familiarize yourself
with the Rancher 2 console and the `dts` production deployment.
The most important details are:

* The service and its supporting executables and configuration data are
  supplied by its Docker image
* The DTS data directory (used for keeping track of ongoing tasks and for
  generating transfer manifests) resides on the NERSC Community File System
  (CFS) under `/global/cfs/cdirs/kbase/dts/`. This volume is visible to the
  service as `/data`.

Let's walk through the process of updating and redeploying the DTS in Spin.

### 1. Update and Push a New Docker Image to Spin

From within a clone of the [DTS git repo](https://github.com/kbase/dit), make
sure the repo is up to date by typing `git pull` in the `main` branch.

Then, sitting in the top-level source directory of your `dts` folder, execute
the `deploy-to-spin.sh` script in the directory containing this `README.md`
file, passing as arguments

1. the name of a tag to identify the new Docker image
2. the name of the NERSC user whose permissions are used for CFS
3. the UID of the NERSC user
4. the group used to determine the user's group permissions
5. the GID of the above group

For example,

```
./path/to/this/dir/deploy-to-spin.sh v1.1 johnson 52710 kbase 54643
```

builds a new DTS Docker image for to be run as the user `johnson`,
with the tag `v1.1`. The script pushes the Docker image to the
[Spin Docker registry](https://registry.spin.nersc.gov). Make sure the tag
indicates the current version of `dts` for clarity.

After building the Docker image and tagging it, the script prompts you for the
NERSC password for the user you specified. This allows it to push the image to
NERSC's Docker image registry, where it can be accessed via the Rancher 2
console.

### 2. Edit the Deployment in Rancher 2 and Restart the Service

Now log in to [Rancher 2](https://rancher2.spin.nersc.gov/login) and
navigate to the `dts` deployment.

1. Click on the `dts` pod to view its status and information
2. Click on the three dots near the right side of the screen and select
   `Edit` to update its configuration.
3. If needed, navigate to the `Volumes` section and edit the CFS directory for
   the volume mounted at `/data`. Usually, this is set to `/global/cfs/cdirs/kbase/dts/`,
   so you don't need to edit this unless you want to place your mapping data
   store file in a different directory.
4. Edit the Docker image for the deployment, changing the tag after the colon
   to match the tag of the Docker image pushed by `deploy-to-spin.sh` above.
5. Make sure that the Scaling/Upgrade Policy is set to `Rolling: stop old pods, then start new`.
   This prevents a new pod from trying to acquire locks on the mapping data
   stores before the old one has released it.
6. Click `Save` to restart the deployment with this new information.

That's it! You've now updated the service with new features and bugfixes.
