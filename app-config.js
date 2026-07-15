/*
 * Resolve a config do cliente (Firebase + API) a partir do subdomínio de acesso.
 * Precisa ser carregado depois de clients-config.js e antes de qualquer script
 * que use window.APP_CONFIG.
 */
(function () {
  const host = window.location.hostname;
  const config = window.CLIENTS_CONFIG[host];

  if (!config) {
    console.error('[app-config] Nenhuma config encontrada para o host "' + host + '".');
  }

  window.APP_CONFIG = config || window.CLIENTS_CONFIG['creta.synciadesk.com.br'];
})();
