# C3 (Cross Cloud Copy)
This is a programme written in GoLang in order to copy objects from one cloud provider to another
Currently, it supports AWS, GCP and AliCloud

## Initiative
With a multi-cloud strategy, we often need to copy objects from one cloud to another.
Each cloud provider has its own data ingestion service. However, if we have multiple clouds in the enterprise,
it will be complicated to set up a data ingestion service on each cloud for the other cloud sources.

So here, we choose to write a small program to copy the objects across different cloud providers easily


## Why GoLang
Some tools like gsutil from GCP allows copying from AWS to GCP, but not to AliCloud.
It's a powerful tool but it is written in Python. As we know, there is a big performance issue with Python's
GIL (Global Interpreter Lock)

We decide to write the program in GoLang hoping to make the copy faster