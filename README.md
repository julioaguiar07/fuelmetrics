# FuelMetrics - Sistema de Análise de Combustíveis

Sistema completo para análise de preços de combustíveis da ANP com backend em Python/FastAPI e frontend moderno.

## 🚀 Funcionalidades

- **Análise em tempo real** dos preços de combustíveis
- **Comparação entre cidades** para otimização de rotas
- **Análise de tendência** e recomendações
- **Simulador de viagem** com cálculo de autonomia
- **Mapa interativo** do Brasil com preços por região
- **Atualização automática** dos dados da ANP

## 🏗️ Arquitetura
fuelmetrics-completo/
├── backend/ # API FastAPI
├── frontend/ # Interface HTML/JS
├── docker-compose.yml
└── README.md


## 🛠️ Instalação

### Método 1: Docker (Recomendado)

```bash
# Clone o repositório
git clone <seu-repositorio>
cd fuelmetrics-completo

# Inicie com Docker Compose
docker-compose up -d

# Acesse:
# Backend API: http://localhost:8000
# Frontend: http://localhost:3000
# Documentação API: http://localhost:8000/api/docs
