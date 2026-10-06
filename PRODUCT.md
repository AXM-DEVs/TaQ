# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

delegated: static HTML, CSS and minimal vanilla JavaScript to keep the project landing page lightweight and dependency-free; no hosting target selected.

## Users

People interested in defensive OSINT and contextual intelligence about brands, domains and organizations, plus potential contributors who want to understand TaQ's current scope and limitations.

## Product Purpose

TaQ is an in-development modular defensive contextual-intelligence engine. Its intended flow is ingest, contextualize, infer, coordinate and learn. The first vertical focuses on public signals about watched brands, domains and organizations.

## Positioning

TaQ aims to carry public events through a traceable pipeline that adds watchlist context, evaluates explicit rules, creates reviewable findings, and records human-approved actions and feedback. Contracts and stages remain modular beyond the initial OSINT vertical.

## Operating Context

The repository is a Rust workspace with local Docker Compose infrastructure. Existing connectors include Wikimedia recent changes, Bluesky Jetstream and configurable RSS. Redpanda transports events; ClickHouse projects event data; PostgreSQL stores state for API and contextual workflows.

## Capabilities and Constraints

- Implemented code covers event ingestion, normalization, deduplication, delivery/replay support, contextualization, rules, findings, approval/outbox, feedback and API/CLI components to varying degrees.
- The project is not through final end-to-end qualification and is not ready to be represented as a production public service.
- Actions write to an outbox; this version does not execute external effects.
- GDELT and CT Logs connectors are not implemented.
- Public use is intended to be free; no commercial plan is intended. Licensing and hosting details are undecided and must not be invented.

## Brand Commitments

Use the name TaQ. The creator is AXM, an independent creator. Explain the project in Spanish for this landing page. Keep product status candid and avoid implying it is already publicly deployed.

## Evidence on Hand

Project documentation and source code in this repository. No approved testimonials, customer logos, benchmark claims, production uptime record, public service URL, or final release qualification evidence is available.

## Product Principles

- Defensive use of public information.
- Explainable and traceable processing.
- Human review before approved actions.
- Honest communication of development status and limitations.
- Free intended public use without invented funding or licensing claims.
