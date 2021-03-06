#!/bin/sh

BASEDIR=`dirname $0`

circleci config process $BASEDIR/config-2_1.yml > $BASEDIR/config.yml.LOWRES

# setup midres
patch -o $BASEDIR/config-2_1.yml.MIDRES $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.mid_res.patch
circleci config process $BASEDIR/config-2_1.yml.MIDRES > $BASEDIR/config.yml.MIDRES
rm $BASEDIR/config-2_1.yml.MIDRES

# setup higher
patch -o $BASEDIR/config-2_1.yml.HIGHRES $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.high_res.patch
circleci config process $BASEDIR/config-2_1.yml.HIGHRES > $BASEDIR/config.yml.HIGHRES
rm $BASEDIR/config-2_1.yml.HIGHRES

# copy lower into config.yml to make sure this gets updated
cp $BASEDIR/config.yml.LOWRES $BASEDIR/config.yml
