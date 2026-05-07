# Project Data Fidelity: Input Data Validation Tracker

Apps Platform service hosting per-dataset camera DQ reports for neural sim input data.

Live at: https://data-fidelity-tracker.experimental.apps.applied.dev

## Adding a new report

1. Generate the report in core-stack:
   ```
   bazel run //tools/offboard/common/image_quality_analysis:neural_sim_report -- \
     --lance_index_path s3://<bucket>/path/to/dataset.lance \
     --title "<Your report title>" \
     --dt_min 2025-07-01 --dt_max <today> \
     --summary_by segment
   ```

2. Copy the output into a new section:
   ```
   cp -r /data/data_quality_report_<date>/  static/sections/<slug>/
   mv static/sections/<slug>/report.html    static/sections/<slug>/index.html
   ```

3. Add a slug entry to `SECTIONS` in `main.py` and a card to `static/hub.html`.

4. Redeploy:
   ```
   apps-platform app deploy
   ```

## Deploying

```
cd <this repo>
apps-platform app deploy
```
