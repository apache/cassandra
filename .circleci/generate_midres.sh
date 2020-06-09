#!/bin/sh

BASEDIR=`dirname $0`

circleci config process $BASEDIR/config-2_1.yml > $BASEDIR/config.yml.LOWRES
patch -o $BASEDIR/config-2_1.yml.MIDRES $BASEDIR/config-2_1.yml $BASEDIR/config-2_1.yml.mid_res.patch
circleci config process $BASEDIR/config-2_1.yml.MIDRES > $BASEDIR/config.yml.MIDRES
rm $BASEDIR/config-2_1.yml.MIDRES
