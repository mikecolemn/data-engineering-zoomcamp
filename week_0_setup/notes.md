
_([Video Link](https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=14))_


# Anaconda

Install Anaconda.  Get the download link for the installed here https://www.anaconda.com/products/distribution, and then use wget to download it

Run `bash ` + the downloaded .sh file.  Follow along the prompts, you can just go with the defaults.

When asked to init, enter yes

Once done, you can do less .bashrc to look at the file that gets executed every time we login.  You'll see anaconda at the bottom.  You can log out and back in, and you'll enter into anaconda into a (base) environment

Enter `source .bashrc` to re-run the bashrc file, rather than logging out and back in


Explore further what Anaconda is

# SSH setup

Create a `config` file in your .ssh folder

Contents
```
Host [host to connect to]
    Hostname [host or ip address here]
    User [username here]
    IdentityFile [path to private ssh key]
```

In your terminal window you can just do `ssh` plus the Host entry, and you'll automatically connect and authenticate

# Docker

apt-get update
apt-get install docker.io


# Visual studio code

Install Remote - SSH from Microsoft

Now, you have a green icon in the lower left of your VS code window, to initiate a remote ssh connection
Click that
Select Connect to Host, and enter your Host you set up in the config file (or anything custom you want)

Some setup will run, may ask you to confirm to continue

You can then Open Folder