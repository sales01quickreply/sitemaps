name: Update Sitemaps Twice Weekly

on:
  schedule:
    # Run every Tuesday and Friday at 2:00 AM UTC (7:30 AM IST)
    - cron: '0 2 * * 2,5'

  # Allow manual triggering from GitHub Actions tab
  workflow_dispatch:
    inputs:
      dry_run:
        description: 'Dry run: build the sitemaps for review only, do not publish them'
        type: boolean
        default: true

jobs:
  update-sitemaps:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          token: ${{ secrets.GITHUB_TOKEN }}

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests>=2.28.0

      # Scheduled runs and manual runs with dry_run unticked publish straight to the repo root.
      # Dry runs write to ./preview instead and attach the files to the run for review.
      - name: Generate sitemaps from Webflow
        env:
          WEBFLOW_API_TOKEN: ${{ secrets.WEBFLOW_API_TOKEN }}
        run: |
          OUT=.
          if [ "${{ inputs.dry_run }}" = "true" ]; then OUT=preview; fi
          python webflow-sitemap.py https://www.quickreply.ai --workers 10 \
            --github-pages-url https://sales01quickreply.github.io/sitemaps \
            --output-dir "$OUT" --previous-dir .

      - name: Attach preview files
        if: inputs.dry_run == true
        uses: actions/upload-artifact@v4
        with:
          name: sitemap-preview
          path: preview/

      - name: Check for changes
        id: check_changes
        if: inputs.dry_run != true
        run: |
          git diff --quiet sitemap*.xml || echo "changes=true" >> $GITHUB_OUTPUT

      - name: Commit and push if changed
        if: inputs.dry_run != true && steps.check_changes.outputs.changes == 'true'
        run: |
          git config --global user.name 'GitHub Actions Bot'
          git config --global user.email 'actions@github.com'
          git add sitemap*.xml
          git commit -m "🤖 Auto-update sitemaps - $(date +'%Y-%m-%d %H:%M UTC')"
          git push

      - name: No changes detected
        if: inputs.dry_run != true && steps.check_changes.outputs.changes != 'true'
        run: |
          echo "✅ No changes detected in sitemaps. Skipping commit."
