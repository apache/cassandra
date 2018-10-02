#!/bin/sh

BASEDIR=`dirname $0`

circleci config process $BASEDIR/config-2_1.yml > $BASEDIR/config.yml.LOWRES
patch -o $BASEDIR/config-2_1.yml.HIGHRES $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.high_res.patch
circleci config process $BASEDIR/config-2_1.yml.HIGHRES > $BASEDIR/config.yml.HIGHRES
rm $BASEDIR/config-2_1.yml.HIGHRES

