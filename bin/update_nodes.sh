#!/bin/bash
while read node; do
    echo "updating gdcdatamodel for node:"
    echo $node
    python update_dictionary_dependency_chain.py --target datamodel --dictionary_commit $node --branch $node
    echo "updating gdcdictionary and datamodel for node:"
    echo $node
    python update_dictionary_dependency_chain.py --target downstream --dictionary_commit $node --datamodel_commit $node --branch $node 
    echo "DONE WITH NODE:"
    echo $node
done < oobleck_nodex.txt

