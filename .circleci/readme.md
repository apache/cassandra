# CircleCI config files

## Switching to high resource settings
This directory contains generated files for high and low resource settings. Switch
between them by copying the correct file to config.yml and committing the result;

`cp .circleci/config.yml.HIGHRES .circleci/config.yml`

Make sure you never edit the config.yml manually.

## Updating the config master
To update the config (other than just swapping high/low resources) you need to install
the [CircleCI CLI](https://circleci.com/docs/2.0/local-cli/#install).

The directory contains `config-2_1.yml` which is then converted to the actual HIGH/LOW
resource files. There is a script called `generate.sh` which creates the LOWRES and
HIGHRES files, read below for details how to do it manually;

1. make your edits to config-2_1.yml - let it stay at lowres settings
1. generate a valid LOWRES file:
   `circleci config process config-2_1.yml > config.yml.LOWRES`
1. then apply the highres patch to config-2_1.yml;
   `patch -o config-2_1.yml.HIGHRES config-2_1.yml config-2_1.yml.high_res.patch`
   (this creates a new file `config-2_1.yml.HIGHRES` instead of in-place patching
   config-2_1.yml)
   Note that if the patch no longer applies to `config-2_1.yml` a new patch file
   is needed, do this by manually making `config-2_1.yml` high resource and create
   the patch file based on the diff (don't commit it though).
1. generate the HIGHRES file:
   `circleci config process config-2_1.yml.HIGHRES > config.yml.HIGHRES`
1. and remove the temporary patched highres `config-2_1.yml.HIGHRES`

