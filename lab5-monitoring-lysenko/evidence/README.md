# Як читати докази

`promtool.json` — справжній stdout/stderr перевірки production rules із2h/10m virtual-time boundary/recovery. `live-alert-states.json` — реальні Prometheus pending/firing/recovery та Grafana inactive/pending/firing; `grafana-runtime-rules.json` окремо підтверджує кінцевий inactive після відновлення.

`prometheus-queries.json`, `efficiency-history.json`, `prometheus-targets.json`, `metrics-*.txt` — відповіді живих endpoint з реальними timestamps. `*-dashboard-api.json` і `grafana-alert-api.json` — отримані назад provisioned objects. `screenshots/*.jpg` — необроблені browser screenshots остаточних dashboards.

`console-live-test.txt` містить живу перевірку алертів, `console-collect.txt` - фінальний збір метрик і результатів.

Числовий звіт побудовано зі збережених результатів; `python lab-support/validate.py` перевіряє їх узгодженість. Для перегляду dashboard наживо потрібно виконати start за README.
