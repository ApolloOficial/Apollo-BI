#### Descrição
<!-- Escreva uma breve explicação do que foi feito na branch -->

---

#### Tipo de alteração
<!--
Apague as opções que não se aplicam a esse PR e preencha a descrição ao lado da que você manteve:

feat: Adição de uma nova funcionalidade
fix: Correção de um bug
docs: Alteração apenas de documentação (README, comentários, etc.)
style: Formatação, espaçamento, ponto e vírgula, sem mudança de lógica
refactor: Reestruturação de código que não corrige bug nem adiciona funcionalidade
perf: Alteração que melhora performance
test: Adição ou correção de testes
build: Mudanças que afetam o build ou dependências externas (ex: package.json)
ci: Mudanças em arquivos e scripts de configuração de CI
chore: Tarefas de manutenção que não alteram código de produção (ex: configs, scripts)
revert: Reversão de um commit anterior
-->
`x`: descrição

---

#### Checklist
<!-- Preencha com ✅ ou ❌. Se não se aplica a este PR, escreva N/A -->

**Segurança**
- [ ] Nenhuma senha, host ou usuário do Neon aparece em notebook, commit ou print
- [ ] Usei o `apollo_connection_neon_core` para conectar ao banco, sem informação em hardcode
- [ ] Dados pessoais (ex: `dim_employee`) não foram levados para a camada `curated`

**Notebooks e pipeline**
- [ ] Criei o notebook seguindo o padrão: título em H2 e descrição clara do que ele faz
- [ ] O notebook roda de cima para baixo (Run all) sem erro
- [ ] As saídas das células foram limpas antes do commit
- [ ] As camadas seguem o padrão `raw`, `cleaned` e `curated`
- [ ] O tratamento feito em `cleaned` (nulos, duplicatas, tipos, valores fora da faixa) está descrito no notebook
- [ ] Se mexi em notebook do pipeline, o Job foi executado de novo e terminou com sucesso

**Dashboard e relatório**
- [ ] Os gráficos usam tabelas da camada `curated`, não a fonte direta
- [ ] Filtros (período e filial) funcionam e os cards mostram o desvio
- [ ] Print do resultado ou da execução do Job anexado como evidência
- [ ] O relatório gerencial foi atualizado, se o fluxo ou os KPIs mudaram

**Git**
- [ ] A branch segue `feat/<nome>-<assunto>` e saiu da `main` atualizada
- [ ] O README foi atualizado, se algo mudou

---

#### Observações
<!-- Preencha apenas se necessário, caso não for, delete o título Observações -->
