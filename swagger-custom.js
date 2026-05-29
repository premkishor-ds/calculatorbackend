window.addEventListener('load', () => {
  const checkInterval = setInterval(() => {
    const swaggerUiEl = document.querySelector('.swagger-ui');
    if (swaggerUiEl) {
      clearInterval(checkInterval);
      createThemeToggle();
    }
  }, 100);

  function createThemeToggle() {
    const savedTheme = localStorage.getItem('swagger-theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    if (savedTheme === 'dark') {
      document.body.classList.add('swagger-dark');
      document.body.classList.remove('swagger-light');
    } else {
      document.body.classList.add('swagger-light');
      document.body.classList.remove('swagger-dark');
    }

    const btn = document.createElement('button');
    btn.id = 'swagger-theme-toggle';
    btn.style.position = 'fixed';
    btn.style.top = '15px';
    btn.style.right = '15px';
    btn.style.zIndex = '9999';
    btn.style.padding = '8px 16px';
    btn.style.border = '1px solid #1f2937';
    btn.style.borderRadius = '10px';
    btn.style.backgroundColor = savedTheme === 'dark' ? '#1f2937' : '#f3f4f6';
    btn.style.color = savedTheme === 'dark' ? '#fbbf24' : '#4f46e5';
    btn.style.cursor = 'pointer';
    btn.style.fontSize = '12px';
    btn.style.fontWeight = '800';
    btn.style.fontFamily = 'sans-serif';
    btn.style.boxShadow = '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)';
    btn.style.transition = 'all 0.2s';
    btn.innerHTML = savedTheme === 'dark' ? '☀️ LIGHT MODE' : '🌙 DARK MODE';

    btn.addEventListener('click', () => {
      const isDark = document.body.classList.contains('swagger-dark');
      if (isDark) {
        document.body.classList.remove('swagger-dark');
        document.body.classList.add('swagger-light');
        localStorage.setItem('swagger-theme', 'light');
        btn.innerHTML = '🌙 DARK MODE';
        btn.style.backgroundColor = '#f3f4f6';
        btn.style.color = '#4f46e5';
        btn.style.borderColor = '#e2e8f0';
      } else {
        document.body.classList.remove('swagger-light');
        document.body.classList.add('swagger-dark');
        localStorage.setItem('swagger-theme', 'dark');
        btn.innerHTML = '☀️ LIGHT MODE';
        btn.style.backgroundColor = '#1f2937';
        btn.style.color = '#fbbf24';
        btn.style.borderColor = '#374151';
      }
    });

    document.body.appendChild(btn);
  }
});
