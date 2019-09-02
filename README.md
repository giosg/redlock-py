# redlock-py

### redlock-cli - Redis distributed locks for cli

Fork of https://github.com/SPSCommerce/redlock-py

**Further Reading:**

* http://redis.io/topics/distlock
* http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
* http://antirez.com/news/101
* https://medium.com/@talentdeficit/redlock-unsafe-at-any-time-40ceac109dbb#.uj9ffh96x


## Installation and development
* Git clone this repo
* Create virtualenv with Python 3.7.x: `mkvirtualenv --python ~/.pyenv/versions/3.7.2/bin/python redlock-py`
* Install requirements: `pip install -r requirements.txt`
* Run tests: `python -m unittest tests.test_redlock`

## Making new release
* Update `setup.py` with new version infromation
* Commit and push to `master`
* Go to github and draft a new release
* Profit!