# 거버넌스 사건 benchmark 사람 라벨 가이드

AI는 후보 추출과 참고 의견에만 사용한다. 정답 파일의 `label_source`는 두 검수자의 독립 판단인 `human` 또는 합의 완료 후의 `adjudicated`만 허용한다.

## Pilot

1. 두 검수자가 서로의 답을 보지 않고 동일한 same-event 50쌍과 core-event 30건을 각각 라벨한다.
2. same-event는 기존 benchmark 계약과 호환되는 `same_story`, `related_but_different`, `different` 중 하나를 고른다. 회사와 주제가 같아도 행위·대상·당사자·효력일·기한 중 알려진 값이 충돌하면 `same_story`가 아니다.
3. core-event는 이 제품의 공식 거버넌스 사건 범위이면 `relevant`, 아니면 `not_relevant`로 표시한다. 일반 업황·주가·실적 언급만으로는 relevant가 아니다.
4. `python -m curator.label_agreement`로 두 작업의 Cohen's kappa를 계산한다. 어느 하나라도 0.8 미만이면 전체 라벨링을 시작하지 않고 가이드와 예시를 수정한 후 새로운 blind pilot을 수행한다.

```powershell
python -m curator.label_agreement `
  --same-story-a pilot/same-a.jsonl --same-story-b pilot/same-b.jsonl `
  --relevance-a pilot/event-a.jsonl --relevance-b pilot/event-b.jsonl `
  --output pilot/agreement.json
```

## 후보 생성과 출시 계약

후보 생성에는 기사 export, 공식 사건 export, 공식 문서 export를 함께 제공한다. 공식 사건은
`document_ids` 중 하나가 문서 export의 `document_id`와 정확히 일치할 때만 표본으로 인정한다.
제목 유사도나 회사명만으로 공식 근거 연결을 추정하지 않는다.

```powershell
python -m curator.benchmark_candidates `
  --articles evidence/articles.jsonl `
  --events evidence/events.jsonl `
  --documents evidence/documents.jsonl `
  --output-dir benchmark-candidates
```

`relevance_candidates.jsonl`은 최종 `task=relevance` 필드 구조를 사용하지만, blind 후보이므로
`label`, `label_source`, `annotator_id`, `labeled_at`은 비어 있다. 검수자는 원문을 바꾸지 않고
이 네 필드만 채운다. `official_event` 표본은 비어 있지 않은 `linked_document_ids`를 유지해야 하고,
`non_governance_hard_negative` 표본은 연결 문서를 주장하지 않으므로 그 배열이 비어 있어야 한다.

출시 benchmark는 다음 조건을 모두 만족해야 한다.

- 공식 문서가 명시적으로 연결된 서로 다른 실제 사건 300개 이상
- 사람이 `not_relevant`로 확인한 `non_governance_hard_negative` 120개 이상
- relevance precision 90% 이상과 recall 95% 이상
- same-event pair 500개 이상과 precision 97% 이상

후보 생성기의 hard-negative 분류는 표본 추출용일 뿐 정답 라벨이 아니다. 같은 회사이면서 공식
사건 문서에 연결되지 않은 기사를 우선 제시하고, 최종 120건은 사람 라벨이 `not_relevant`인 행만
계산한다. 표본이 부족하면 `--allow-partial`을 정식 증빙에 사용하지 말고 입력 export를 보강한다.

## 전체 라벨과 합의

- 후보 생성기는 650쌍(예상 양성 300, hard-negative 250, easy-negative 100)과 실제 공식 사건 300건 이상을 준비하지만 라벨은 비워 둔다.
- 두 검수자가 전체 표본을 blind labeling한 뒤 불일치 목록만 함께 검토한다.
- 합의한 행은 `adjudicated`로 기록한다. 합의할 수 없는 행만 제품책임자가 판정하며 판정 사유를 별도 비공개 대장에 남긴다.
- 최종 benchmark에는 article/document pair 500개 이상, 실제 사건 300개 이상, 비관련 hard-negative 120개 이상이 있어야 한다.
- 원문 제목·본문은 수정하거나 번역하지 않는다. 평가자 개인정보는 benchmark 파일에 넣지 않고 비식별 `annotator_id`와 별도 보관대장 reference만 사용한다.
