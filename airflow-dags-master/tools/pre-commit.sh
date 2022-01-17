#!/bin/bash
files=$(ls -R notebooks | awk '
/:$/&&f{s=$0;f=0}
/:$/&&!f{sub(/:$/,"");s=$0;f=1;next}
NF&&f{ print s"/"$0 }' | grep .ipynb)

for f in "${files[@]}"
do
  jupyter nbconvert --ClearOutputPreprocessor.enabled=True --inplace $f
  git add $f
done