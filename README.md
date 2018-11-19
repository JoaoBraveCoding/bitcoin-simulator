# Bitcoin Simulator

This Bitcoin Simulator was built on top of SimpleDA (https://github.com/miguelammatos/SimpleDA)

This simulator is able to simulate two distinct behaviours of Bitcoin, the relay of information (transactions and blocks) and the membership protocol (not yet fully implemented).

All configurations are specified in the respectives conf.yaml, files regarding relay of information can be found in the conf_diss folder and files regarding membership protocol in the conf_memb folder.


## Running

Packages required
- difflib
- yaml
- cPickle
- numpy

How to run:
- Use PyPy (tested with 2.7.13), otherwise simulations take too long
- Configuration is described in an yaml file called conf.yaml
- Invocation pypy (diss or memb).py <conf_directory> <run_number>
```
 $ pypy diss.py conf_diss/ 1
```
- This is CPU and RAM bound, if you have enough cores and RAM it's better
  to run several processes in parallel, for instance:
```
 $ for i in {1..10}; do
 $ time $pypy diss.py conf_diss/ $i > conf_diss/run-$i.log $
 $ done
```
- Then you can compute the required stats for all runs just by iterating over the produced results.
  utils.py provides some common statistical functions and other utils such as dumping to GnuPlot format.
- The diss.py file already provides some stats that used during my masters


## License
Copyright (C) 2018 João Marçal

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see https://www.gnu.org/licenses/.
