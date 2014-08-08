# Displays the list of available actions
help:
	@echo "\n Available tasks:"
	@echo "\t rc_check      - checks if REDCap is running"
	@echo "\t rc_list       - list the id of the default test project"
	@echo "\t rc_save       - save a backup of REDCap database"
	@echo "\t rc_clean      - remove data for default test project"
	@echo "\t rc_clean_hcv  - truncates the extra 'hcv' tables such as: _hcvt2_ae, ..."
	@echo "\t rc_clean_who  - truncates the extra 'who' tables such as: _whodrug_atc, ..."
	@echo "\t rc_post       - send test data to REDCap"
	@echo "\t rc_get        - get stored test data from REDCap"
	@echo "\t rc_fresh      - erase/insert/retrieve test data from REDCap"
	@echo "\t rc_get_rate   - shows the config option value: 'page_hit_threshold_per_minute' "
	@echo "\t rc_set_rate   - set the value for: 'page_hit_threshold_per_minute' "
	@echo "\t egg_test      - deploy the redi egg file to the vagrant box and run redi"
	

rc_check:
	curl -s http://localhost:8998/redcap/ | grep -i  'Welcome\|Critical Error' 

rc_list:
	vagrant ssh -c 'cd /vagrant/scripts && php redcapdbm.php -l | grep hcvtarget_20_development'

rc_save:
	vagrant ssh -c 'cd /vagrant/scripts && php redcapdbm.php -b'

rc_clean:
	vagrant ssh -c 'cd /vagrant/scripts && php redcapdbm.php -d 12'	

rc_clean_hcv:
	vagrant ssh -c 'mysql -uroot -ppassword redcap -e " truncate _hcvt2_ae; truncate _hcvt2_cm; truncate _hcvt2_dm; truncate _hcvt2_ie; truncate _hcvt2_lb; truncate _hcvt2_mh; truncate _hcvt2_su; truncate _hcvt2_suppae; truncate _hcvt2_suppcm; truncate _hcvt2_suppdm; truncate _hcvt2_suppds; truncate _hcvt2_suppfa; truncate _hcvt2_suppie; truncate _hcvt2_supplb; truncate _hcvt2_suppmh; truncate _hcvt2_suppsu; truncate _hcvt2_suppvs; truncate _test_view_dm; truncate _tvrsp_ae;" ' 

rc_clean_who:
	vagrant ssh -c 'mysql -uroot -ppassword redcap -e " truncate _whodrug_atc; truncate _whodrug_dd; truncate _whodrug_dda; truncate _whodrug_ddsource; truncate _whodrug_ina; truncate _whodrug_mp; truncate _whodrug_mp_us; truncate _whodrug_thg; " '

rc_demographics:
	../bin/utils/redcap_records.py --token=121212 --url=http://localhost:8998/redcap/api/ -i demographic_test_data.csv

rc_post:
	python ../bin/redi.py

rc_get:
	../bin/utils/redcap_records.py --token=121212 --url=http://localhost:8998/redcap/api/ -f "demgraphics chemistry"

rc_fresh:
	make rc_clean
	make rc_demographics
	make rc_post
	make rc_get

rc_get_rate:
	vagrant ssh -c 'mysql -uroot -ppassword -e "select * FROM redcap.redcap_config WHERE field_name = \"page_hit_threshold_per_minute\" "'

rc_set_rate:
	# echo $(filter-out $@,$(MAKECMDGOALS))
	vagrant ssh -c 'mysql -uroot -ppassword -e "UPDATE redcap.redcap_config SET value = $(filter-out $@,$(MAKECMDGOALS)) WHERE field_name = \"page_hit_threshold_per_minute\" "'

test_egg:
	make egg_test
	
egg_test:
	@test -f ../dist/REDI*.egg || (echo 'Please execute "make egg" from the project root first'			&& exit 1)
	@test -d config				|| (echo 'Please create a "config" folder with necessary files first'	&& exit 1)
	cp ../dist/REDI*.egg .
	vagrant ssh -c 'sudo easy_install /vagrant/REDI*.egg'
	make rc_clean
	make rc_demographics
	vagrant ssh -c 'redi -c /vagrant/config'
