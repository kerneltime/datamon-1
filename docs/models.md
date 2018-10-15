# Models

Models are the tasks that get executed when an event matches. These have a separate lifecycle from the data repositories.

Models are run in runtimes, this a catalog of pre-built runtimes in which models can get executed. This allows for the operations team to update the runtimes independently of the code that is running in those runtimes.

The requirements for building a runtime will be discussed later, for now we'll define the operations that are required for a model.  In this form the deployment is tightly coupled with a git repository.  But doesn't function as a code repository but rather as a 

## Deploy

Models can live anywhere on a file system and the only thing that is required for them to function is that they have a manifest describing how to package the files.  When the model does live in a git repository, it can't be deployed with a dirty git tree.  So we can only package files when they are already known to git and have been committed with no changes pending.  

Deployment 

```sh
tpt model deploy --config path/to/config --context path/to/project/root
```

This deploy command can be broken up in to several distinct and more granular commands:

* verify
* package
* upload
* run

## Archive

Once deployed we can't actually delete a model anymore, but we can deactivate a model.
