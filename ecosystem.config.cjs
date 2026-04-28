module.exports = {
  apps: [
    {
      name: "slack-bot",
      script: "Slack Bot/slack_bot.py",
      // pythonw.exe로 변경 — 콘솔 창 제거 (CMD 창 hide)
      interpreter: "D:\\Vibe Dev\\Slack Bot\\venv\\Scripts\\pythonw.exe",
      cwd: "D:\\Vibe Dev\\Slack Bot",
      args: "--commands-only",
      windowsHide: true,
      env: {
        PYTHONIOENCODING: "utf-8",
      },
      // 자동 재시작: 크래시 시 즉시 복구
      autorestart: true,
      max_restarts: 10,
      min_uptime: "10s",
      restart_delay: 5000,       // 5초 대기 후 재시작
      // 로그
      error_file: "logs/slack-bot-error.log",
      out_file: "logs/slack-bot-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      merge_logs: true,
      // 감시 비활성화 (코드 변경 시 수동 재시작)
      watch: false,
    },
  ],
};
