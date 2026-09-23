# pi-cron daemon — install as a systemd user service
#
#   make install   — install + enable + start ~/.config/systemd/user/pi-cron.service
#   make status    — show service state
#   make uninstall — stop + remove the unit
#   make logs      — tail the daemon log

PREFIX      ?= $(HOME)/.local
PYTHON      ?= $(shell command -v python3)
DAEMON      := $(abspath extensions/daemon.py)
UNIT_DIR    := $(HOME)/.config/systemd/user
UNIT        := $(UNIT_DIR)/pi-cron.service
CRON_DIR    ?= $(HOME)/.local/state/pi-cron

.PHONY: install uninstall status logs

install:
	@test -n "$(PYTHON)" || { echo "python3 not found"; exit 1; }
	@mkdir -p $(UNIT_DIR) $(CRON_DIR)/jobs
	@printf '%s\n' \
	  "[Unit]" \
	  "Description=pi-cron — self-contained job scheduler for pi agents" \
	  "After=network-online.target" \
	  "" \
	  "[Service]" \
	  "Type=simple" \
	  "Environment=PI_CRON_DIR=$(CRON_DIR)" \
	  "Environment=PATH=%h/.local/bin:%h/.npm-global/bin:/usr/local/bin:/usr/bin:/bin" \
	  "ExecStart=$(PYTHON) $(DAEMON)" \
	  "Restart=always" \
	  "RestartSec=10" \
	  "" \
	  "[Install]" \
	  "WantedBy=default.target" > $(UNIT)
	systemctl --user daemon-reload
	systemctl --user enable --now pi-cron.service
	@echo "installed: $(UNIT)"
	@echo "jobs dir:  $(CRON_DIR)/jobs"

uninstall:
	-systemctl --user disable --now pi-cron.service
	-rm -f $(UNIT)
	-systemctl --user daemon-reload
	@echo "removed (jobs kept in $(CRON_DIR))"

status:
	@systemctl --user status pi-cron.service --no-pager || true

logs:
	@tail -f $(CRON_DIR)/daemon.log
