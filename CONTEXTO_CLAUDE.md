# Contexto do Projeto — Portal Creta

Cole este arquivo no início de uma nova conversa com Claude para continuar o desenvolvimento.

---

## Regras obrigatórias

1. **Dados sensíveis**: nunca enviar dados financeiros para serviços de terceiros. Credenciais ficam apenas nas variáveis de ambiente do Cloud Run, nunca no repositório.
2. **Perguntar antes de implementar features novas**. Correções de bugs podem ser feitas sem perguntar.
3. **Sempre fornecer comandos git em blocos bash** com sequência completa incluindo push.
4. **Deploy backend e frontend são separados**: `git push` = GitHub Pages (frontend); `gcloud run deploy` = Cloud Run (backend).

---

## Infraestrutura — multi-tenant (synciadesk.com.br)

O sistema é vendido pra múltiplos escritórios ("clientes"). Cada cliente tem seu
**próprio projeto GCP + Firebase isolados** (BigQuery, Firestore, Cloud Run, GCS)
— zero dados compartilhados entre clientes. O **frontend é um único deploy
compartilhado** (Firebase Hosting, projeto `synciadesk-hosting`), que serve
todos os subdomínios (`creta.synciadesk.com.br`, `ewz.synciadesk.com.br`, ...)
com o mesmo conjunto de arquivos estáticos.

A config de cada cliente (Firebase + URL da API) não fica mais commitada no
repo — cada página carrega `<script src="/config.js"></script>`, que o
`firebase.json` reescreve pro serviço **`config-api`** (Cloud Run, também no
projeto `synciadesk-hosting`). Esse serviço lê o header `X-Forwarded-Host`
(o domínio real que o visitante usou) e devolve a config certa, buscando no
Firestore desse mesmo projeto (coleção `clients`, um documento por
subdomínio). Do ponto de vista de cada página HTML nada muda — continua sendo
um `<script src>` síncrono, só que gerado na hora em vez de estático.

Cadastro de cliente novo (a parte de config) é feito em **`admin.synciadesk.com.br`**
— login com Firebase Auth do projeto Creta (`creta-btg-bd3a8`), restrito aos
e-mails em `ADMIN_EMAILS`. Ele chama `GET/POST/DELETE /api/clients` do
`config-api` (mesmo rewrite de Hosting, `/api/**`). **Provisionar
infraestrutura nova** (criar projeto GCP/Firebase, BigQuery, Cloud Run)
continua manual — ver "Como adicionar um cliente novo" abaixo.

| Serviço | Valor |
|---|---|
| Frontend (todos os clientes) | Firebase Hosting, projeto `synciadesk-hosting` — múltiplos domínios customizados |
| Config por cliente (runtime) | Firestore `clients/{hostname}` no projeto `synciadesk-hosting`, servido via `config-api` (`config-api/main.py`) em `/config.js` |
| Admin de clientes | `admin.synciadesk.com.br` (`admin.html`) — login com Auth da Creta, CRUD via `/api/clients` |
| Config por cliente (backend/deploy) | `clients/<slug>.env` — um arquivo por cliente, lido pelo `scripts/deploy-backend.sh` |
| Faturamento | Todos os projetos GCP (incluindo `synciadesk-hosting`) usam a conta **TechUm** (`01F53C-019FE6-2C4878`) — ela tem cota de poucos projetos linkados, pode precisar desvincular um projeto parado antes de linkar um novo |

### Cliente: Creta

| Serviço | Valor |
|---|---|
| Domínio | `creta.synciadesk.com.br` (em migração; `techumgit.github.io/portal-creta` mantido como alias durante a transição) |
| Backend API | Cloud Run: `https://api-creta-978599698367.us-central1.run.app` |
| GCP Project (Cloud Run) | `creta-btg` (project number 978599698367) |
| Firebase Project | `creta-btg-bd3a8` (project number 609636823379) |
| BigQuery | project `creta-btg`, dataset `dados_crus`, tabela `posicao_das_contas` |
| Firestore | project `creta-btg-bd3a8` |
| Pasta local | `~/Desktop/Portal Escritorio AAI/` |

### Comandos de deploy

```bash
# Frontend (Firebase Hosting) — atualiza TODOS os clientes de uma vez
cd ~/Desktop/Portal\ Escritorio\ AAI && git add -A && git commit -m "mensagem" && git push
firebase deploy --only hosting

# Backend — um cliente específico ou todos (lê clients/*.env)
cd ~/Desktop/Portal\ Escritorio\ AAI && ./scripts/deploy-backend.sh creta
cd ~/Desktop/Portal\ Escritorio\ AAI && ./scripts/deploy-backend.sh all
```

### Como adicionar um cliente novo

**Infraestrutura (manual — ideia é fazer isso num notebook Colab à parte,
pra manter esse passo sob controle direto em vez de automatizado):**

**Automatizado — `./scripts/provisionar-cliente.sh <slug>`:**

Roda direto no terminal (usa a autenticação local do `gcloud`/`firebase`, sem
login interativo). Faz tudo isso, em ordem, e é seguro rodar de novo se
travar no meio (pula o que já existe):

1. Cria o projeto GCP e vincula a conta de faturamento TechUm.
2. Ativa as APIs necessárias (Firestore, Cloud Run, Cloud Build, BigQuery,
   Cloud Scheduler, etc.).
3. Concede as permissões do service account padrão (Cloud Build + acionar
   Cloud Run Jobs).
4. Cria o projeto Firebase (vinculado ao GCP) + Firestore.
5. Cria o bucket GCS do pipeline.
6. Cria o dataset BigQuery e copia o **schema** (não os dados) das tabelas
   da Creta.
7. Pede os secrets (`btg-client-id`, `btg-client-secret`, `admin-emails`) via
   prompt escondido no terminal.
8. Deploy do backend (`api-creta`, Cloud Run service).
9. Deploy dos jobs automatizados (`job-posicoes`, `job-suitability`,
   `job-carteira-recomendada`, Cloud Run Jobs).
10. Cria os 3 gatilhos do Cloud Scheduler (posições: 8h dias úteis;
    suitability e carteira recomendada: 8h segunda) — mesmo padrão da Creta.
11. Pega a config web do Firebase (cria o app Web se não existir).

**Manual, o que sobra (contas/DNS que o script não controla):**

- No console Firebase do projeto `synciadesk-hosting` → Hosting → "Adicionar
  domínio customizado" → `<slug>.synciadesk.com.br`; criar os registros DNS
  indicados no registro.br.
- No console Firebase do projeto do cliente → Authentication → Sign-in
  method → ativar Email/Senha.
- No console Firebase do projeto do cliente → Authentication → Settings →
  Authorized domains → adicionar `<slug>.synciadesk.com.br`.
- Criar o primeiro usuário admin do cliente: cria o usuário no Firebase Auth
  do projeto dele + grava `users/{uid}` no Firestore com `role: admin` +
  define os custom claims (`{"role": "admin"}`) — mesmo padrão usado em
  `api-creta/criar_usuarios.py`. Depois desse primeiro login, o resto dos
  usuários pode ser criado pela própria tela de gestão de usuários dentro do
  portal (`configuracoes.html`).
- Colocar o logo do cliente em `logos/<slug>.svg` (ou `.png`), commitar e
  rodar `firebase deploy --only hosting` — fica acessível em
  `https://synciadesk-hosting.web.app/logos/<arquivo>` (ver `logos/README.md`).
- Login em `admin.synciadesk.com.br` (e-mail precisa estar em `ADMIN_EMAILS`
  tanto em `api-creta/main.py` quanto em `config-api/main.py`) → "Novo
  cliente" → preencher subdomínio, URL da API (impressa no resumo final do
  script), nome de exibição, URL do logo (passo acima) e a config do
  Firebase (etapa 11). Salvar já deixa o `/config.js` daquele subdomínio no
  ar — sem `displayName`/`logoUrl`, a página ficaria com a marca da Creta,
  então esses dois campos são obrigatórios no formulário. Não precisa de
  commit nem `firebase deploy` pra essa parte (só pro logo em si, que é
  arquivo estático).

Nota: o cliente `interno` (`interno-portal`) é uma exceção deliberada — ele
roda com `ANONYMIZE=true` e **sem** os gatilhos do Cloud Scheduler (dados
atualizados sob demanda antes de demonstrações, rodando
`scripts/copiar-e-mascarar-historico.py` manualmente). Não é o padrão pra
clientes de verdade.

---

## Cliente `interno` (interno-portal) — ambiente de testes/demo

Projeto GCP/Firebase separado (`interno-portal`), alimentado com **dados
reais do BTG da Creta** (mesmas credenciais, copiadas via Secret Manager),
mas com **anonimização automática** — não é bug se os números não baterem
com os da Creta, é proposital.

- `job-posicoes` roda lá com `ANONYMIZE=true` + secret `ANON_SALT` (só
  existe nesse projeto). A máscara (`job-posicoes/anonimizar.py`) troca
  número de conta e nome de assessor por um pseudônimo estável (hash
  determinístico com o salt), e escala valores monetários por um fator fixo
  por conta (0.6–1.4x) — mantém o padrão dos dados sem expor o real.
- Histórico já carregado foi copiado da Creta com
  `scripts/copiar-e-mascarar-historico.py <projeto-destino>` (lê
  `creta-btg.dados_crus`, aplica a mesma máscara, grava no destino,
  `WRITE_TRUNCATE`). Precisa do `ANON_SALT` do projeto destino como env var
  pra rodar (mesmo valor do secret `anon-salt`, senão os pseudônimos não
  batem com os que o job-posicoes vai gerar dali pra frente).
- Importante: `Conta` tem formatos diferentes entre tabelas (STRING com
  espaço/zero à esquerda em `posicao_das_contas`, INTEGER limpo em
  `conta_assessor_base`/`receitas_para_repasse`) — a máscara normaliza antes
  de gerar o hash (`_normalizar_conta`), senão a mesma conta vira pseudônimos
  diferentes em tabelas diferentes e quebra o JOIN entre elas.

---

## Arquitetura

- **Auth**: Firebase Auth com JWT claims (`role`, `assessor_name`, `uid`)
- **Roles**:
  - `admin` — acesso total
  - `backoffice` — acesso total **exceto** receitas/relatórios/evolução do cliente
  - `assessor` — vê apenas seus próprios clientes
- **Backend**: FastAPI no Cloud Run, `api-creta/main.py`
- **Firestore collections**: `pipeline_clientes`, `movimentacoes`, `noticias`, `prioridades`, `portal_usuarios`

---

## Páginas HTML (todas em `~/Desktop/Portal Creta/`)

| Arquivo | Descrição |
|---|---|
| `dashboard.html` | Visão geral, KPIs, prioridades da semana |
| `gestao.html` | Alertas de carteira (caixa parado, vencimentos, queda AuC, sem receita) |
| `pipeline.html` | CRM de oportunidades, log de contatos |
| `posicoes.html` | Portfólio do cliente, posições e movimentações |
| `receitas.html` | Receitas e repasses — **admin only** |
| `relatorios.html` | Histórico de carteiras — **admin only** |
| `chamados.html` | Chamados internos, separados por status/prioridade |
| `aprovacoes.html` | Aprovações de e-mail (Tesouro Direto, Fundos c/ Carência, Derivativos) |
| `produtos.html` | Produtos disponíveis + recomendações do comitê |
| `opcoes.html` | Opções estruturadas |
| `configuracoes.html` | Exceções, arquivos do pipeline, gestão de usuários |

---

## Backend — endpoints principais (`api-creta/main.py`)

### Receita/relatório — **admin only** (backoffice recebe 403)
- `GET /api/receitas`
- `GET /api/detalhe`
- `GET /api/evolucao_cliente`
- `GET /api/relatorio/historico`

### Acesso privilegiado (admin + backoffice) — `PRIVILEGED_ROLES = {"admin", "backoffice"}`
- `GET /api/gestao` — alertas de carteira
- `GET /api/posicoes` — posições
- `GET /api/posicoes/{conta}/produtos`
- `GET /api/posicao` — snapshot
- `GET /api/opcoes`
- `GET /api/pipeline` — CRM
- `GET /api/assessores`
- `GET /api/usuarios` — lista usuários do Firebase
- `GET /api/config/excecoes`
- `GET/POST /api/pipeline/arquivos`
- `GET/POST /api/comite/recomendacoes`
- `GET/POST /api/produtos-manuais`

### Usuários — **admin only**
- `POST /api/usuarios` — cria usuário Firebase
- `PATCH /api/usuarios/{uid}` — atualiza role/assessor_name

### Outros
- `GET /api/chamados`, `POST /api/chamados`, `PATCH /api/chamados/{doc_id}`
- `GET/POST/PUT/DELETE /api/aprovacoes`
- `GET/POST /api/movimentacoes/{conta}`
- `POST /api/webhook/btg`

---

## Padrões importantes no código

### Verificação de role no backend
```python
PRIVILEGED_ROLES = {"admin", "backoffice"}
REVENUE_ROLES    = {"admin"}

# Em endpoints privilegiados (não-receita):
is_admin = role in PRIVILEGED_ROLES

# Em endpoints de receita:
if role not in REVENUE_ROLES:
    raise HTTPException(status_code=403, detail="Acesso restrito a administradores.")
```

### Auth no frontend (padrão da maioria das páginas)
```javascript
const claims = (await user.getIdTokenResult()).claims;
const role   = claims.role || 'assessor';
_isAdmin = (role === 'admin' || role === 'backoffice');
// BackOffice: oculta links de receita/relatorio
if (role === 'backoffice') {
  document.querySelectorAll('a[href="receitas.html"],a[href="relatorios.html"]')
    .forEach(el => el.style.display = 'none');
}
```

### Token nas chamadas API
```javascript
async function getIdToken() {
  return await firebase.auth().currentUser.getIdToken();
}
// Uso:
fetch(`${API_URL}/api/...`, { headers: { Authorization: `Bearer ${await getIdToken()}` } })
```

---

## Funcionalidades implementadas (resumo)

- **Dashboard**: KPIs reais (AuC, contas ativas), Prioridades da Semana + Lista de Prioridades lado a lado
- **Gestão**: alertas com filtro (quedas AuC < R$10k e vencimentos < R$1k não aparecem); clicar no nome abre modal de carteira; clicar nos chips abre modal de detalhe do alerta
- **Pipeline**: formulário com NNM + AuC Global; filtro de assessor populado do Firestore; log de contatos com último contato e próximo FUP; campo OffShore (antigo COE)
- **Meu Portfólio**: modal de produto com largura 1280px, categorias em accordion
- **Chamados**: separados em ativos/concluídos, ordenados por prioridade
- **Aprovações**: tipos Tesouro Direto, Fundos c/ Carência, Derivativos (com badge amarelo)
- **Configurações**: gestão de usuários (listar, criar, editar role/assessor_name); badge BackOffice amarelo
- **Usuários Firestore**: `portal_usuarios/{uid}` sincronizado ao listar (fallback quando Firebase Auth list_users falha por IAM)

---

## IAM configurado

```bash
# Service account do Cloud Run:
978599698367-compute@developer.gserviceaccount.com

# Permissão de Firebase Auth Admin concedida no projeto Firebase:
gcloud projects add-iam-policy-binding creta-btg-bd3a8 \
  --member="serviceAccount:978599698367-compute@developer.gserviceaccount.com" \
  --role="roles/firebaseauth.admin"

# Identity Toolkit API habilitada em:
gcloud services enable identitytoolkit.googleapis.com --project=creta-btg-bd3a8
```

---

## Firestore — estrutura das collections

```
pipeline_clientes/{id}
  nome, conta_btg, conta_antiga, assessor_name, assessor_uid,
  stage, pipe_quente, auc_btg, auc_global, nnm,
  ultimo_contato, proximo_fup, observacoes: []

movimentacoes/{id}
  conta, tipo, valor, data, descricao

prioridades/admin  (e /slugify(assessor_name))
  items: [{texto, concluido}]

portal_usuarios/{uid}
  uid, email, nome, role, assessor_name

chamados/{id}
  titulo, descricao, prioridade, status, assessor_uid, assessor_name, created_at

aprovacoes/{id}
  cliente_nome, cliente_email, tipo, valido_de, valido_ate,
  assessor_uid, assessor_name, status, created_at
```

---

## Como continuar

Ao iniciar nova conversa, cole este arquivo e diga o que quer fazer. Claude vai ter todo o contexto necessário para continuar sem repetir perguntas sobre infraestrutura ou padrões já definidos.
