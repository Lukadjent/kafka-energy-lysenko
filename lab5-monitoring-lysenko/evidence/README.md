# Як читати докази

`promtool.json` — справжній stdout/stderr перевірки production rules із2h/10m virtual-time boundary/recovery. `live-alert-states.json` — реальні Prometheus pending/firing/recovery та Grafana inactive/pending/firing; `grafana-runtime-rules.json` окремо підтверджує кінцевий inactive після відновлення.

`prometheus-queries.json`, `efficiency-history.json`, `prometheus-targets.json`, `metrics-*.txt` — відповіді живих endpoint з реальними timestamps. `*-dashboard-api.json` і `grafana-alert-api.json` — отримані назад provisioned objects. `screenshots/*.jpg` — необроблені browser screenshots остаточних dashboards.

`console-all.txt` містить початкову помилку старту Grafana (нестворений каталог SQLite та некоректний min_interval). У поточному configure() це виправлено. Після перезапуску виконано `console-live-test.txt` і фінальний `console-collect.txt`; попередні collect збережені як console-collect-previous-*. Вони не видаються за окремі production load trials.

Числовий звіт побудовано з фінального набору evidence; `lab-support/validation.json` підтверджує його узгодженість. Сервери після збору доказів штатно/керовано зупинено; для перегляду dashboard наживо потрібно знову виконати start за README.
