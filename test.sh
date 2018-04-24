#!/bin/bash
echo "Testing..."
for i in `find test -type f -iname '*.input'`; do 
	./bin/main < $i > $(echo $i | sed s/.input/.output/);

	if diff -q -b -B $(echo $i | sed s/.input/.output/) $(echo $i | sed s/.input/.expected/) >/dev/null 2>&1
	then
		echo -e "\e[39m"$i "\e[32mPASSED"
	else
		echo -e "\e[39m"$i "\e[31mFAIL"
	fi
done

