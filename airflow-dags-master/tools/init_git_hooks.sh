#!/bin/sh
pip install nbconvert
cp tools/pre-commit.sh .git/hooks/pre-commit
chmod 777 .git/hooks/pre-commit