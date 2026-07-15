/*
 * Config pública por cliente (subdomínio). Não contém segredos — a config web
 * do Firebase é destinada a ser pública (a segurança fica nas Security Rules
 * e nas claims do JWT verificadas no backend).
 *
 * Para adicionar um cliente novo: crie o projeto GCP + Firebase dedicado
 * (veja "Como adicionar um cliente novo" no CONTEXTO_CLAUDE.md), depois
 * adicione uma entrada aqui com o hostname do subdomínio dele.
 */
const CRETA_CONFIG = {
  firebaseConfig: {
    apiKey:            "AIzaSyDLJrg7Cmaq6DDlDdcLfe4kH9hUukMVuE0",
    authDomain:        "creta-btg-bd3a8.firebaseapp.com",
    projectId:         "creta-btg-bd3a8",
    storageBucket:     "creta-btg-bd3a8.firebasestorage.app",
    messagingSenderId: "609636823379",
    appId:             "1:609636823379:web:fbf9de8c2960d7392c31a6",
    measurementId:     "G-WQSQD9CMJF"
  },
  apiUrl: "https://api-creta-978599698367.us-central1.run.app"
};

window.CLIENTS_CONFIG = {
  'creta.synciadesk.com.br': CRETA_CONFIG,
  // Alias temporário — mantém o link antigo funcionando durante a migração de domínio.
  'techumgit.github.io': CRETA_CONFIG,
  // Dev local.
  'localhost': CRETA_CONFIG,
  '127.0.0.1': CRETA_CONFIG
};
