{{/*
Expand the name of the chart.
*/}}
{{- define "r-kafka.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "r-kafka.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart label selector
*/}}
{{- define "r-kafka.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "r-kafka.labels" -}}
helm.sh/chart: {{ include "r-kafka.chart" . }}
{{ include "r-kafka.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "r-kafka.selectorLabels" -}}
app.kubernetes.io/name: {{ include "r-kafka.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name
*/}}
{{- define "r-kafka.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "r-kafka.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Image reference
*/}}
{{- define "r-kafka.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
Headless service name
*/}}
{{- define "r-kafka.headlessName" -}}
{{- printf "%s-headless" (include "r-kafka.fullname" .) }}
{{- end }}

{{/*
Generate quorum peers string for KRaft configuration.
Each pod gets: "{brokerId + ordinal}@{pod-dns}:{controllerPort}"
*/}}
{{- define "r-kafka.quorumPeers" -}}
{{- $fullname := include "r-kafka.fullname" . }}
{{- $headless := include "r-kafka.headlessName" . }}
{{- $ns := .Release.Namespace }}
{{- $port := .Values.broker.controllerPort }}
{{- $startId := .Values.broker.startId }}
{{- $peers := list }}
{{- range $i := until (int .Values.replicaCount) }}
{{- $bid := add $startId $i }}
{{- $dns := printf "%s@%s-%d.%s.%s.svc.cluster.local:%d" (toString $bid) $fullname $i $headless $ns $port }}
{{- $peers = append $peers $dns }}
{{- end }}
{{- join ", " $peers }}
{{- end }}
